import os
import time
import fcntl
import pickle
import socket
import signal
import sqlite3
import logging
import hashlib
import asyncio
import argparse
import mimetypes
import urllib.parse
from logging import critical as log


async def rpc(server, req):
    reader, writer = await asyncio.open_connection(server[0], server[1])

    writer.write(b'\n')
    writer.write(pickle.dumps(req))
    await writer.drain()

    res = pickle.loads((await reader.read()))
    writer.close()

    tmp = res.copy()
    tmp.pop('value', None)
    log('server%s %s', server, tmp)

    res['server'] = server
    return res


async def mrpc(server_req_map):
    tasks = [rpc(k, v) for k, v in server_req_map.items()]

    responses = dict()
    for res in await asyncio.gather(*tasks, return_exceptions=True):
        if type(res) is dict:
            responses[res['server']] = res

    return responses


def rpc_filter(status, responses):
    return {k: v for k, v in responses.items() if v['status'] == status}


def read_log(req, db):
    row = db.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    if row and row[1] is None and row[2] is None:
        return dict(status='ok', key=row[3], value=row[4])

    return dict(status='notfound')


def read_log_state(req, db):
    row = db.execute('''select promised, accepted
                        from log where seq=?
                     ''', (req['seq'],)).fetchone()

    if row and row[0] is None and row[1] is None:
        return dict(status='ok')

    return dict(status='notfound')


def read_key_state(req, db):
    rows = db.execute('''select seq, promised, accepted
                         from log
                         where key=? order by seq desc
                      ''', (req['key'],)).fetchall()

    for row in rows:
        if row[1] is None and row[2] is None:
            return dict(status='ok', seq=row[0])

    return dict(status='notfound')


def paxos_promise(req, db):
    db.execute('update log set key=0 where seq=0')

    row = db.execute('''select seq, promised, accepted from log
                        order by seq desc limit 1
                     ''').fetchone()

    seq = row[0]
    if row[1] is None and row[2] is None:
        seq = row[0] + 1
        db.execute('insert into log(seq,promised,accepted) values(?,0,0)',
                   (seq,))

    row = db.execute('select * from log where seq=?', (seq,)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        return dict(status='InvalidSeq')

    key_seq = None
    if req['key'] is not None:
        tmp = db.execute('''select seq from log where key=?
                            order by seq desc limit 1
                         ''', (req['key'],)).fetchone()
        key_seq = tmp[0] if tmp else 0

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    db.execute('update log set promised=? where seq=?', (req['promised'], seq))

    db.execute('update log set key=null where seq=0')
    db.commit()

    return dict(status='ok', seq=seq, accepted=row[2],
                key=row[3], value=row[4], key_seq=key_seq)


def paxos_accept(req, db):
    db.execute('update log set key=0 where seq=0')

    row = db.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    if not row:
        db.execute('insert into log(seq) values(?)', (req['seq'],))
    elif row[0] is None or req['promised'] < row[0]:
        return dict(status='InvalidSeq')

    # All good. Accept this proposal. If a quorum reaches this step, this value
    # is learned. This seq -> value mapping is now permanent.
    # Proposer would detect that a quorum reached this stage and would in the
    # next learn phase, record this fact by setting promised/accepted as NULL.
    db.execute('''update log set promised=?, accepted=?, key=?, value=?
                  where seq=?
               ''', (req['promised'], req['promised'],
                     req['key'], req.pop('value'), req['seq']))

    db.execute('update log set key=null where seq=0')
    db.commit()

    return dict(status='ok')


def paxos_learn(req, db):
    db.execute('update log set key=0 where seq=0')

    if 'value' in req:
        db.execute('delete from log where seq=?', (req['seq'],))
        db.execute('insert into log(seq, key, value) values(?,?,?)',
                   (req['seq'], req['key'], req.pop('value')))
    else:
        db.execute('update log set promised=null, accepted=null where seq=?',
                   (req['seq'],))

    db.execute('update log set key=null where seq=0')
    db.commit()

    return dict(status='ok')


async def paxos_propose(req):
    quorum = int(len(ARGS.servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time())

    # Promise Phase
    responses = await mrpc({
        s: dict(action='promise', db=req['db'], promised=ts, key=req['key'])
        for s in ARGS.servers})

    tmp = rpc_filter('ok', responses)
    counts = dict()
    responses = dict()
    for k, v in tmp.items():
        counts.setdefault(v['seq'], list()).append(k)

        if len(counts[v['seq']]) == quorum:
            seq = v['seq']
            responses = {x: y for x, y in tmp.items() if seq == y['seq']}

    if len(responses) < quorum:
        return dict(status='NoPromiseQuorum')

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    proposal = (0, req['key'], req['value'])
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted'] and res['accepted'] > proposal[0]:
            proposal = (res['accepted'], res['key'], res['value'])

    # Optimistic Locking
    if 0 == proposal[0] and req['version'] is not None:
        tmp = [v['key_seq'] for v in responses.values()
               if v['key_seq'] is not None]
        tmp = max(tmp) if tmp else 0
        if tmp != req['version']:
            return dict(status='Conflict', seq=tmp)

    # Accept Phase
    responses = await mrpc({s: dict(action='accept', db=req['db'], seq=seq,
                                    promised=ts,
                                    key=proposal[1], value=proposal[2])
                            for s in ARGS.servers})
    responses = rpc_filter('ok', responses)

    if len(responses) < quorum:
        return dict(status='NoAcceptQuorum')

    # Learn Phase
    requests = dict()
    for s in ARGS.servers:
        requests[s] = dict(action='learn', db=req['db'], seq=seq)

        if s in responses:
            requests[s].update(dict(promised=ts))
        else:
            requests[s].update(dict(key=proposal[1], value=proposal[2]))

    await mrpc(requests)

    return dict(status='ok' if 0 == proposal[0] else 'ProposalConflict',
                seq=seq if 0 == proposal[0] else 0)


def server():
    # This would avoid creation of any zombie processes
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', ARGS.port))
    sock.listen()
    log('server started on port(%d)', ARGS.port)

    while True:
        conn, addr = sock.accept()

        if os.fork():
            # Parent process. Continue waiting for next client connection
            # Closing as this connection would be handled by the child process
            conn.close()
        else:
            # Child process. Close the listening socket
            sock.close()
            break

    # This is forked child process. We will serve just one request here.

    fd = conn.makefile(mode='rb')
    line = fd.readline().decode().strip()

    # HTTP Server
    if line:
        _, url, _ = line.split(' ')
        db, key = urllib.parse.unquote(url.strip('/')).split('/')

        hdr = dict()
        while True:
            line = fd.readline().strip()
            if not line:
                break
            k, v = line.decode().split(':', 1)
            hdr[k.lower()] = v.strip()

        version = int(hdr.get('if-none-match', '"0"')[1:-1])
        status, ver, value = call_sync(get(ARGS.servers, db, key, version))

        if status != 'ok':
            conn.sendall('HTTP/1.1 404 Not Found\n\n'.encode())
        elif ver == version:
            conn.sendall('HTTP/1.1 304 Not Modified\n'.encode())
            conn.sendall('ETag: "{}"\n\n'.format(ver).encode())
        else:
            mime_type = mimetypes.guess_type(key)[0]
            mime_type = mime_type if mime_type else 'text/plain'
            conn.sendall('HTTP/1.1 200 OK\nETag: "{}"\n'.format(ver).encode())
            conn.sendall('Content-Type: {}\n'.format(mime_type).encode())
            conn.sendall('Content-Length: {}\n\n'.format(len(value)).encode())
            conn.sendall(value)

        return conn.close()

    # RPC Server
    req = pickle.load(fd)
    db = sqlite3.connect('paxolite.' + req['db'] + '.sqlite3')

    if 'propose' == req['action']:
        fcntl.flock(os.open('paxolite.propose.lock', os.O_CREAT | os.O_RDONLY),
                    fcntl.LOCK_EX)
        res = call_sync(paxos_propose(req))
        req.pop('value')

    elif req['action'] in ('promise', 'accept', 'learn'):
        if addr[0] in [ip for ip, port in ARGS.servers]:
            res = dict(
                learn=paxos_learn,
                accept=paxos_accept,
                promise=paxos_promise,
            )[req['action']](req, db)

    elif req['action'] in ('read_log', 'read_log_state', 'read_key_state'):
        res = dict(
            read_log=read_log,
            read_log_state=read_log_state,
            read_key_state=read_key_state
        )[req['action']](req, db)

    else:
        res = dict(status='NotAllowed')

    req.update(res)
    conn.sendall(pickle.dumps(req))
    conn.close()

    req.pop('value', None)
    log('client%s %s', addr, req)


async def put(servers, db, value, key=None, version=0):
    srvs = [(hashlib.md5((db + str(s)).encode()).digest(), s) for s in servers]

    for _, s in sorted(srvs):
        try:
            res = await rpc(s, dict(action='propose', db=db, key=key,
                                    version=version, value=value))

            if 'ok' != res['status']:
                return dict(status=res['status'])

            return dict(status=res['status'], version=res['seq'])
        except Exception:
            pass


async def sync(servers, db, seq=1):
    # Exponential backoff
    sleep_sec = 0

    while True:
        sleep_sec = max(1, min(15, 2*sleep_sec))

        try:
            res = await mrpc({s: dict(action='read_log_state', db=db, seq=seq)
                              for s in servers})

            srvs = set(servers) - set(rpc_filter('ok', res))

            for srv in rpc_filter('ok', res):
                r = await rpc(srv, dict(action='read_log', db=db, seq=seq))

                await mrpc({s: dict(action='learn', db=db, seq=seq,
                                    key=r['key'], value=r['value'])
                            for s in srvs})

                print((time.strftime('%H:%M:%S'), seq, r['key'],
                       len(r['value']), srvs))

                seq += 1
                sleep_sec = 0
                break
        except Exception:
            pass

        if sleep_sec > 0:
            print('sleeping for {} sec'.format(sleep_sec))
            time.sleep(sleep_sec)


async def get(servers, db, key, existing_version=0):
    quorum = int(len(servers)/2) + 1

    responses = await mrpc({s: dict(action='read_key_state', db=db, key=key)
                            for s in servers})

    if len(responses) < quorum:
        return dict(status='noquorum')

    if len(rpc_filter('notfound', responses)) >= quorum:
        return dict(status='notfound')

    responses = rpc_filter('ok', responses)

    max_seq = max([v['seq'] for v in responses.values()])

    if max_seq == existing_version:
        return dict(status='ok', version=max_seq, value=None)

    for k, v in responses.items():
        if v['seq'] == max_seq:
            res = await rpc(k, dict(action='read_log', db=db, seq=max_seq))
            return dict(status='ok', version=max_seq, value=res['value'])


def call_sync(obj):
    return asyncio.get_event_loop().run_until_complete(obj)


def init(db, passwd):
    db = sqlite3.connect('paxolite.' + db + '.sqlite3')

    db.execute('''create table if not exists log(
        seq      integer primary key autoincrement,
        promised integer,
        accepted integer,
        key      text,
        value    blob)''')
    db.execute('create index if not exists i1 on log(key, seq)')
    db.execute('insert into log(seq, value) values(0,?)',
               (passwd,))
    db.commit()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--db', dest='db', default='default')
    ARGS.add_argument('--passwd', dest='passwd')
    ARGS.add_argument('--sync', dest='sync', type=int)

    ARGS.add_argument('--key', dest='key')
    ARGS.add_argument('--value', dest='value')
    ARGS.add_argument('--version', dest='version', type=int)

    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--servers', dest='servers',
                      default='127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,'
                              '127.0.0.2:5003,127.0.0.1:5004')
    ARGS = ARGS.parse_args()

    ARGS.servers = [(s.split(':')[0].strip(), int(s.split(':')[1]))
                    for s in ARGS.servers.split(',')]

    if ARGS.port:
        server()
    elif ARGS.passwd:
        init(ARGS.db, ARGS.passwd)
    elif ARGS.sync:
        call_sync(sync(ARGS.servers, ARGS.db, ARGS.sync))
    elif ARGS.value:
        if ARGS.version:
            ARGS.version = int(ARGS.version)

        print(call_sync(put(ARGS.servers, ARGS.db,
                            ARGS.value.encode(), ARGS.key, ARGS.version)))
    else:
        print(call_sync(get(ARGS.servers, ARGS.db, ARGS.key, ARGS.version)))
