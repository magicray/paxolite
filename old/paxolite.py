import time
import json
import pickle
import sqlite3
import asyncio
import logging
import hashlib
import argparse
import mimetypes
import urllib.parse
from logging import critical as log


# Utility methods
def response(x, status, **kwargs):
    x.update(kwargs)
    x['status'] = status
    return x


async def _rpc(server, req):
    reader, writer = await asyncio.open_connection(server[0], server[1])

    value = req.pop('value', None)
    if value:
        req['value'] = len(value)

    writer.write(json.dumps(req).encode())
    writer.write(b'\n')
    if value:
        writer.write(value)

    await writer.drain()

    res = json.loads((await reader.readline()).decode())

    log('server%s rpc(%s)', server, res)
    value = res.pop('value', None)
    if value:
        res['value'] = await reader.read(value)

    writer.close()

    res['__server__'] = server
    return res


async def rpc(server_req_map):
    tasks = [_rpc(k, v) for k, v in server_req_map.items()]

    responses = dict()
    for res in await asyncio.gather(*tasks, return_exceptions=True):
        if type(res) is dict and 'ok' == res['status']:
            responses[res.pop('__server__')] = res

    return responses


# Find out the log seq for the next log entry to be filled.
def get_next_seq():
    row = DB.execute('''select seq, promised from log
                        order by seq desc limit 1''').fetchone()

    # It is either the max seq in the db if it is not yet learned,
    # or the max seq+1 if the max entry is already learned.
    if row:
        return row[0]+1 if row[1] is None else row[0]

    # Nothing yet in the db, lets start our log with seq number 1
    return 1


def read_stats(req):
    return response(req, 'ok', seq=get_next_seq())


def read_log(req):
    DB.rollback()

    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    if row and row[1] is None and row[2] is None:
        return response(req, 'ok', checksum=row[3], value=row[4])

    return response(req, 'ok')


def read_kv(req):
    DB.rollback()

    row = DB.execute('select * from kv where key=?', (req['key'],)).fetchone()
    if row:
        return response(req, 'ok', version=row[1], value=row[2])

    return response(req, 'ok', version=0)


def check_kv_table(blob):
    for key, version, value in pickle.loads(blob):
        if version:
            row = DB.execute('select version from kv where key=?',
                             (key,)).fetchone()
            if row[0] != version:
                return False

    return True


def update_kv_table(seq, blob):
    for key, version, value in pickle.loads(blob):
        DB.execute('delete from kv where key=?', (key,))
        if value:
            DB.execute('insert into kv values(?,?,?)', (key, seq, value))

    return True


def paxos_promise(req):
    seq = get_next_seq()

    # Insert a new row if it does not already exis
    row = DB.execute('select * from log where seq=?', (seq,)).fetchone()
    if not row:
        # Make and entry in the log now
        DB.execute('insert into log(seq, promised, accepted) values(?,?,?)',
                   (seq, 0, 0))
        row = DB.execute('select * from log where seq=?', (seq,)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        return response(req, 'InvalidSeq', seq=seq)

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    DB.execute('update log set promised=? where seq=?', (req['promised'], seq))
    DB.commit()

    return response(req, 'ok', seq=seq, accepted=row[2], value=row[4])


def paxos_accept(req):
    row = DB.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in the promise phase. Reject.
    if not row or req['promised'] != row[0]:
        return response(req, 'InvalidPromise')

    if check_kv_table(req['value']) is not True:
        return response(req, 'TxnConflict')

    # All good. Accept this proposal. If a quorum reaches this step, this value
    # is learned. This seq -> value mapping is now permanent.
    # Proposer would detect that a quorum reached this stage and would in the
    # next learn phase, record this fact by setting promised/accepted as NULL.
    DB.execute('update log set accepted=?, value=? where seq=?',
               (req['promised'], req.pop('value'), req['seq']))
    DB.commit()

    return response(req, 'ok')


def compute_checksum(seq, value):
    chksum = DB.execute('select checksum from log where seq=?',
                        (seq-1,)).fetchone()
    chksum = chksum[0] if chksum and chksum[0] else ''

    checksum = hashlib.md5()
    checksum.update(chksum.encode())
    checksum.update(value)
    return checksum.hexdigest()


def paxos_learn(req):
    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    # We did not participate in promise/accept phase. Reject.
    if not row or req['promised'] != row[1]:
        return response(req, 'InvalidPromise')

    checksum = compute_checksum(req['seq'], row[4])
    DB.execute('''update log
                  set promised=null, accepted=null, checksum=?
                  where seq=?
               ''', (checksum, req['seq']))
    update_kv_table(req['seq'], row[4])
    DB.commit()

    return response(req, 'ok')


async def paxos_propose(value):
    quorum = int(len(ARGS.servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time()*10**6)

    # Promise Phase
    responses = await rpc({s: dict(action='promise', promised=ts)
                           for s in ARGS.servers})
    if len(responses) < quorum:
        return 'NoPromiseQuorum', 0

    seq = max([v['seq'] for v in responses.values()])

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    proposal = (0, value)
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted'] > proposal[0] and res['seq'] == seq:
            proposal = (res['accepted'], res['value'])

    # Accept Phase
    responses = await rpc({s: dict(action='accept', seq=seq, promised=ts,
                                   value=proposal[1])
                           for s in responses})
    if len(responses) < quorum:
        return 'NoAcceptQuorum', 0

    # Learn Phase
    responses = await rpc({s: dict(action='learn', seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoLearnQuorum', 0

    return ('ok', seq) if 0 == proposal[0] else ('ProposalConflict', 0)


async def sync():
    while True:
        responses = await rpc({s: dict(action='read_stats')
                               for s in ARGS.servers})

        max_srv, max_seq = None, 0
        for k, v in responses.items():
            if v['seq'] > max_seq:
                max_srv, max_seq = k, v['seq']

        DB.rollback()
        seq = get_next_seq()

        # Update the servers that has fallen behind
        while seq < max_seq:
            res = await rpc({max_srv: dict(action='read_log', seq=seq)})

            if not res or 'value' not in res[max_srv]:
                break

            checksum = compute_checksum(seq, res[max_srv]['value'])
            assert(checksum == res[max_srv]['checksum'])

            DB.execute('delete from log where seq=? and promised is not null',
                       (seq,))
            DB.execute('insert into log values(?,null,null,?,?)',
                       (seq, checksum, res[max_srv]['value']))
            update_kv_table(seq, res[max_srv]['value'])
            DB.commit()

            seq = get_next_seq()

        await asyncio.sleep(1)


async def server(reader, writer):
    line = await reader.readline()
    method = line.decode().split(' ')[0].lower()

    # HTTP Server
    if 'get' == method:
        _, key, _ = line.decode().split(' ')
        key = urllib.parse.unquote(key[1:])

        hdr = dict()
        while True:
            line = (await reader.readline()).strip()
            if not line:
                break
            k, v = line.decode().split(':', 1)
            hdr[k.lower()] = v.strip()

        version = int(hdr.get('if-none-match', '"0"')[1:-1])
        status, ver, chksum, value = await Client().get(key, version)

        if status != 'ok':
            writer.write('HTTP/1.1 404 Not Found\n\n'.encode())
        elif ver == version:
            writer.write('HTTP/1.1 304 Not Modified\n'.encode())
            writer.write('ETag: "{}"\n\n'.format(ver).encode())
        else:
            mime_type = mimetypes.guess_type(key)[0]
            mime_type = mime_type if mime_type else 'text/plain'
            writer.write('HTTP/1.1 200 OK\nETag: "{}"\n'.format(ver).encode())
            writer.write('Content-Type: {}\n'.format(mime_type).encode())
            writer.write('Content-Length: {}\n\n'.format(len(value)).encode())
            writer.write(value)

        await writer.drain()
        return writer.close()

    # RPC Server
    cluster_ip_list = [s[0] for s in ARGS.servers]
    if writer.get_extra_info('peername')[0] not in cluster_ip_list:
        log('client%s not allowed', writer.get_extra_info('peername'))
        return writer.close()

    req = json.loads(line)

    value = req.pop('value', None)
    if value:
        req['value'] = await reader.read(value)

    res = dict(
        read_kv=read_kv,
        read_log=read_log,
        read_stats=read_stats,
        learn=paxos_learn,
        accept=paxos_accept,
        promise=paxos_promise,
    )[req['action']](req)

    value = res.pop('value', None)
    if value:
        res['value'] = len(value)

    writer.write(json.dumps(res).encode())
    writer.write(b'\n')
    if value:
        writer.write(value)

    await writer.drain()
    writer.close()

    log('client%s rpc(%s)', writer.get_extra_info('peername'), res)


async def timeout():
    timeout = int(time.time()) % min(ARGS.timeout, 3600)
    log('will exit after sec(%d)', timeout)
    await asyncio.sleep(timeout)

    row = DB.execute('select max(seq), min(seq), count(*) from log').fetchone()
    if row:
        new_min = row[1] + int((row[2] * timeout) / 86400)
        DB.execute('delete from log where seq<?', (new_min,))
        log('totol max(%d) min(%d) now(%d)', row[1], row[0], new_min)

    log('exiting after sec(%d)', timeout)


class Client():
    def __init__(self, servers=None):
        self.servers = servers if servers else ARGS.servers

    async def get(self, key, existing_version=0):
        quorum = int(len(self.servers)/2) + 1

        responses = await rpc({s: dict(action='read_stats')
                               for s in self.servers})

        if len(responses) < quorum:
            return 'NoQuorum', 0, b''

        seq = max([v['seq'] for v in responses.values()])
        srvrs = [(hashlib.md5(str(time.time()*10**9).encode()).digest(), s)
                 for s in [k for k, v in responses.items() if v['seq'] == seq]]

        for _, srv in sorted(srvrs):
            responses = await rpc({srv: dict(action='read_kv', key=key)})
            for k, v in responses.items():
                if 0 == v['version']:
                    return 'notfound', 0, b''

                if existing_version == v['version']:
                    return 'ok', v['version'], b''

                return 'ok', v['version'], v['value']

    async def put(self, key_version_value_list):
        return await paxos_propose(pickle.dumps(key_version_value_list))

    def sync(self, async_callable):
        return asyncio.get_event_loop().run_until_complete(async_callable)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--key', dest='key')
    ARGS.add_argument('--file', dest='file')
    ARGS.add_argument('--value', dest='value')
    ARGS.add_argument('--version', dest='version', type=int, default=0)

    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--servers', dest='servers',
                      default='127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,'
                              '127.0.0.2:5003,127.0.0.1:5004')
    ARGS.add_argument('--timeout', dest='timeout', type=int, default=30)

    ARGS = ARGS.parse_args()

    ARGS.servers = [(s.split(':')[0].strip(), int(s.split(':')[1]))
                    for s in ARGS.servers.split(',')]

    # Start the server
    if ARGS.port:
        DB = sqlite3.connect('paxolite.db')
        DB.execute('''create table if not exists kv(
            key      text primary key,
            version  unsigned integer,
            value    blob)''')
        DB.execute('''create table if not exists log(
            seq      unsigned integer primary key,
            promised unsigned integer,
            accepted unsigned integer,
            checksum text,
            value    blob)''')

        asyncio.ensure_future(asyncio.start_server(server, '', ARGS.port))
        log('listening on port(%s)', ARGS.port)
        asyncio.ensure_future(sync())
        asyncio.get_event_loop().run_until_complete(
                asyncio.ensure_future(timeout()))
        exit()

    # Client - CLI
    if not ARGS.key:
        # This is only for testing - Pick random key and value
        ARGS.key = time.strftime('%y%m%d.%H%M%S.') + str(time.time()*10**6)
        ARGS.value = ARGS.key

    client = Client()
    if ARGS.file or ARGS.value:
        if ARGS.file:
            with open(ARGS.file, 'rb') as fd:
                value = fd.read()
        else:
            value = ARGS.value.encode()

        print(client.sync(client.put([(ARGS.key, ARGS.version, value)])))
    else:
        print(client.sync(client.get(ARGS.key)))
