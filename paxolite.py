import os
import time
import hmac
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


def create_schema(db):
    db = sqlite3.connect('paxolite.' + db + '.sqlite3')
    db.execute('''create table if not exists log(
        seq      integer primary key autoincrement,
        promised integer,
        accepted integer,
        key      text,
        value    blob)''')
    db.execute('create index if not exists i1 on log(key, seq)')
    db.execute('create index if not exists i2 on log(promised, seq)')
    return db


def paxos_promise(req, db):
    db.execute('update log set key=0 where seq=0')

    seq = req['seq']

    row = db.execute('select seq from log where seq=?', (seq,)).fetchone()
    if not row:
        db.execute('insert into log(seq,promised,accepted) values(?,0,0)',
                   (seq,))

    row = db.execute('select * from log where seq=?', (seq,)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if row[1] is None or req['promised'] <= row[1]:
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

    return dict(status='ok', accepted=row[2], key=row[3], value=row[4],
                key_seq=key_seq)


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


async def paxos_propose(req, db):
    quorum = int(len(ARGS.servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time())

    if 'seq' in req:
        seq = req['seq']
    else:
        # Find out the next available seq number in the log
        responses = await mrpc({s: dict(action='read_info', db=req['db'])
                                for s in ARGS.servers})
        responses = rpc_filter('ok', responses)

        if len(responses) < quorum:
            return dict(status='NoInfoQuorum')

        seq = max([v['next_seq'] for v in responses.values()])

    # Authecation - calculate HMAC of seq with the secret code
    auth = hmac.new(
        db.execute('select value from log where seq=0').fetchone()[0].encode(),
        str(seq).encode(), hashlib.sha512).hexdigest()

    # Promise Phase
    responses = await mrpc({s: dict(action='promise', db=req['db'], seq=seq,
                                    promised=ts, key=req['key'], auth=auth)
                            for s in ARGS.servers})
    responses = rpc_filter('ok', responses)

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
                                    promised=ts, auth=auth,
                                    key=proposal[1], value=proposal[2])
                            for s in ARGS.servers})
    responses = rpc_filter('ok', responses)

    if len(responses) < quorum:
        return dict(status='NoAcceptQuorum')

    # Learn Phase
    requests = dict()
    for s in ARGS.servers:
        requests[s] = dict(action='learn', db=req['db'], seq=seq, auth=auth)

        if s in responses:
            requests[s].update(dict(promised=ts))
        else:
            requests[s].update(dict(key=proposal[1], value=proposal[2]))

    await mrpc(requests)

    return dict(status='ok' if 0 == proposal[0] else 'ProposalConflict',
                seq=seq if 0 == proposal[0] else 0)


async def repair_log(req, db):
    # Find out holes and incomplete paxos entries in the log
    rows = db.execute('''select seq+1 from log
                         where seq+1 not in (select seq from log)
                         union
                         select seq from log
                         where promised is not null
                      ''').fetchall()

    for seq in sorted([row[0] for row in rows])[:-1]:
        learned = False

        for srv in ARGS.servers:
            r = await rpc(srv, dict(action='read_log', db=req['db'], seq=seq))

            if 'value' in r:
                # Log entry successfully fetched, update the db
                paxos_learn(dict(seq=seq, key=r['key'], value=r['value']), db)
                learned = True
                break

        if not learned:
            await paxos_propose(dict(db=req['db'], seq=seq,
                                     key=None, value=None), db)


async def server():
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

    # Trivial HTTP Server
    # Supports only GET method with ETag header
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
        r = await get(ARGS.servers, db, key, version)

        if 'ok' != r['status']:
            conn.sendall('HTTP/1.1 404 Not Found\n\n'.encode())
        elif version == r['version']:
            conn.sendall('HTTP/1.1 304 Not Modified\n'.encode())
            conn.sendall('ETag: "{}"\n\n'.format(r['version']).encode())
        else:
            mime_type = mimetypes.guess_type(key)[0]
            mime_type = mime_type if mime_type else 'text/plain'
            conn.sendall('HTTP/1.1 200 OK\n'.encode())
            conn.sendall('ETag: "{}"\n'.format(r['version']).encode())
            conn.sendall('Content-Type: {}\nContent-Length: {}\n\n'.format(
                mime_type, len(r['value'])).encode())
            conn.sendall(r['value'])

        return conn.close()

    # RPC Server
    req = pickle.load(fd)
    db = sqlite3.connect('paxolite.' + req['db'] + '.sqlite3')

    if 'propose' == req['action']:
        fcntl.flock(os.open('paxolite.propose.lock', os.O_CREAT | os.O_RDONLY),
                    fcntl.LOCK_EX)
        res = await paxos_propose(req, db)
        req.pop('value')

    elif req['action'] in ('promise', 'accept', 'learn'):
        if addr[0] in [ip for ip, port in ARGS.servers]:
            # Authecation - calculate HMAC of seq with the secret code
            key = db.execute('select value from log where seq=0').fetchone()[0]
            auth = hmac.new(key.encode(), str(req['seq']).encode(),
                            hashlib.sha512).hexdigest()

            if req['auth'] == auth:
                res = dict(
                    learn=paxos_learn,
                    accept=paxos_accept,
                    promise=paxos_promise,
                )[req['action']](req, db)
            else:
                res = dict(status='unauthorized')

    elif 'read_log' == req['action'] and req['seq'] > 1:
        row = db.execute('select * from log where seq=?',
                         (req['seq'],)).fetchone()

        res = dict(status='notfound')
        if row and row[1] is None and row[2] is None:
            res = dict(status='ok', key=row[3], value=row[4])

    elif 'read_key' == req['action']:
        rows = db.execute('''select seq, promised, accepted
                             from log
                             where key=? order by seq desc
                          ''', (req['key'],))

        res = dict(status='notfound')
        for row in rows:
            if row[1] is None and row[2] is None:
                res = dict(status='ok', seq=row[0])
                break

    elif 'read_info' == req['action']:
        row = db.execute('''select seq, promised, accepted from log
                            order by seq desc limit 1
                         ''').fetchone()

        res = dict(status='ok', next_seq=row[0])

        if row[1] is None and row[2] is None:
            res['next_seq'] = row[0] + 1

    else:
        res = dict(status='notallowed')

    req.update(res)
    conn.sendall(pickle.dumps(req))
    conn.close()

    req.pop('auth', None)
    req.pop('value', None)
    log('client%s %s', addr, req)

    try:
        if 'learn' != req['action']:
            return

        fcntl.flock(os.open('paxolite.repair.lock', os.O_CREAT | os.O_RDONLY),
                    fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Fix inconsistencies in the log
        await repair_log(req, db)
    except Exception:
        pass


async def rpc(server, req):
    reader, writer = await asyncio.open_connection(server[0], server[1])

    writer.write(b'\n')
    writer.write(pickle.dumps(req))
    await writer.drain()

    res = pickle.loads((await reader.read()))
    writer.close()

    tmp = res.copy()
    tmp.pop('auth', None)
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


async def put(servers, db, value, key=None, version=0):
    # This requires some explanation.
    #
    # We want only one server to drive the writes, as conflicts in
    # paxos rounds lead to live-lock like situation, significantly
    # delaying writes. We try servers in a fixed sequence, to ensure
    # requests land on the same server from all the clients.
    #
    # To still distribute load evenly, the sequence of server we follow for
    # each db is different.
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

    return dict(status='unavailable')


async def get(servers, db, key, existing_version=0):
    quorum = int(len(servers)/2) + 1

    responses = await mrpc({s: dict(action='read_key', db=db, key=key)
                            for s in servers})

    if len(responses) < quorum:
        return dict(status='noquorum')

    if len(rpc_filter('notfound', responses)) >= quorum:
        return dict(status='notfound')

    responses = rpc_filter('ok', responses)

    seq = max([v['seq'] for v in responses.values()])

    if seq == existing_version:
        return dict(status='ok', version=seq, value=None)

    for k, v in responses.items():
        if v['seq'] == seq:
            r = await rpc(k, dict(action='read_log', db=db, seq=seq))
            return dict(status='ok', version=seq, value=r['value'])

    return dict(status='unavailable')


async def watch(servers, src, timeout):
    db = create_schema(src)
    seq = 2
    row = db.execute('select max(seq) from log').fetchone()

    if row[0]:
        seq = row[0] + 1

    while True:
        sleep = True

        for i in range(len(servers)):
            try:
                srv = servers[int(time.time()*10**6) % len(servers)]
                r = await rpc(srv, dict(action='read_log', db=src, seq=seq))

                if 'value' in r:
                    db.execute('insert into log(seq,key,value) values(?,?,?)',
                               (seq, r['key'], r['value']))
                    db.commit()

                    print('time({}) seq({}) key({}) bytes({})'.format(
                        time.strftime('%H:%M:%S'), seq, r['key'],
                        len(r['value'])))

                    seq += 1
                    sleep = False
                    break
            except Exception:
                pass

        if sleep:
            time.sleep(int(time.time()) % timeout)


def sync(obj):
    return asyncio.get_event_loop().run_until_complete(obj)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--db', dest='db', default='default')
    ARGS.add_argument('--passwd', dest='passwd')
    ARGS.add_argument('--timeout', dest='timeout', type=int, default=5)

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
        sync(server())
    elif ARGS.passwd:
        db = create_schema(ARGS.db)
        db.execute('insert into log(seq,value) values(0,?)', (ARGS.passwd,))
        db.commit()
    elif ARGS.value:
        print(sync(put(ARGS.servers, ARGS.db, ARGS.value.encode(), ARGS.key,
                       int(ARGS.version) if ARGS.version else ARGS.version)))
    elif ARGS.key:
        print(sync(get(ARGS.servers, ARGS.db, ARGS.key, ARGS.version)))
    else:
        sync(watch(ARGS.servers, ARGS.db, ARGS.timeout))
