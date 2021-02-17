import time
import json
import sqlite3
import asyncio
import logging
import hashlib
import argparse
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


# Cache frequently read key/value here. First read is from sqlite but
# subsequent reads are going to be very fast, as we don't need to go to disk.
class CACHE():
    key = dict()
    value = dict()


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


def read_value(req):
    return response(req, 'ok', value=CACHE.value[req['seq']])


def read_key(req):
    if req['key'] not in CACHE.key:
        DB.rollback()

        rows = DB.execute('select * from log where key=?', (req['key'],))

        for row in filter(lambda row: row[1] is None, rows.fetchall()):
            CACHE.key[req['key']] = row[0]
            CACHE.value[row[0]] = row[4]

    if req['key'] in CACHE.key:
        return response(req, 'ok', seq=CACHE.key[req['key']])

    return response(req, 'NotFound')


def read_log(req):
    DB.rollback()

    row = DB.execute('select * from log where seq>=? order by seq limit 1',
                     (req['seq'],)).fetchone()

    if row and row[1] is None and row[2] is None:
        return response(req, 'ok', seq=row[0], key=row[3], value=row[4])

    return response(req, 'ok', seq=0)


async def get_value(key):
    quorum = int(len(ARGS.servers)/2) + 1

    # Get the best log_seq to be used
    responses = await rpc({s: dict(action='read_key', key=key)
                          for s in ARGS.servers})
    if len(responses) < quorum:
        return 'NoQuorum', 0, b''

    seq = max([v['seq'] for v in responses.values()])
    srv = [k for k, v in responses.items() if v['seq'] == seq]
    srv = sorted([(hashlib.md5(str(time.time()*10**9).encode()).digest(), s)
                 for s in srv])
    for _, s in srv:
        responses = await rpc({s: dict(action='read_value', seq=seq)})
        for k, v in responses.items():
            return 'ok', seq, v['value']


def paxos_promise(req):
    DB.rollback()

    # This log_seq would create a hole in the log. Terminate.
    if req['seq'] != get_next_seq():
        return response(req, 'InvalidSeq')

    # Insert a new row if it does not already exist for this log seq
    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()
    if not row:
        # Make and entry in the log now
        DB.execute('insert into log(seq, promised, accepted) values(?,?,?)',
                   (req['seq'], 0, 0))
        row = DB.execute('select * from log where seq=?',
                         (req['seq'],)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        return response(req, 'SmallerPromiseSeq', existing=row[1])

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    DB.execute('update log set promised=? where seq=?',
               (req['promised'], req['seq']))
    DB.commit()

    return response(req, 'ok', accepted=row[2], key=row[3], value=row[4])


def paxos_accept(req):
    DB.rollback()

    row = DB.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in the promise phase. Reject.
    if not row or req['promised'] != row[0]:
        return response(req, 'PromiseSeqMismatch')

    # Optimistic locking. Proceed only if the existing seq is same as specified
    # by client. Terminate otherwise.
    if req['version'] > 0:
        rows = DB.execute('''select seq from log
                             where key=? and promised is null
                          ''', (req['key'],)).fetchall()

        version = rows[0][0] if rows else 0

        if version != req['version']:
            return response(req, 'VersionMismatch')

    # All good. Accept this proposal. If a quorum reaches this step, this value
    # is learned. This seq -> (key,val) mapping is now permanent.
    # Proposer would detect that a quorum reached this stage and would in the
    # next learn phase, record this fact by setting promised/accepted as NULL.
    DB.execute('update log set accepted=?, key=?, value=? where seq=?',
               (req['promised'], req['key'], req.pop('value'), req['seq']))
    DB.commit()

    return response(req, 'ok')


def paxos_learn(req):
    DB.rollback()

    row = DB.execute('select promised, key from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in promise/accept phase. Reject.
    if not row or req['promised'] != row[0]:
        return response(req, 'PromiseSeqMismatch')

    DB.execute('delete from log where seq<? and key=?', (req['seq'], row[1]))
    DB.execute('update log set promised=null, accepted=null where seq=?',
               (req['seq'],))
    DB.commit()

    CACHE.value.pop(CACHE.key.pop(row[1], 0), None)  # Clear the cache

    return response(req, 'ok')


async def sync():
    while True:
        DB.rollback()

        responses = await rpc({s: dict(action='read_stats')
                               for s in ARGS.servers})

        max_srv, max_seq = None, 0
        for k, v in responses.items():
            if v['seq'] > max_seq:
                max_srv = k
                max_seq = v['seq']

        seq = get_next_seq()
        if seq < max_seq:
            to_be_deleted = seq

        # Update the servers that has fallen behind
        while seq < max_seq:
            res = await rpc({max_srv: dict(action='read_log', seq=seq)})

            if not res or 0 == res[max_srv]['seq']:
                break

            if to_be_deleted:
                DB.execute('delete from log where seq=?', (to_be_deleted,))
                to_be_deleted = None

            res = res[max_srv]
            DB.execute('delete from log where key=?', (res['key'],))
            DB.execute('insert into log values(?,null,null,?,?)',
                       (res['seq'], res['key'], res['value']))
            DB.commit()

            seq = res['seq'] + 1

        await asyncio.sleep(1)


async def server(reader, writer):
    line = await reader.readline()
    method = line.decode().split(' ')[0].lower()

    # HTTP Server
    if method in ('get', 'put', 'post'):
        _, key, _ = line.decode().split(' ')

        hdr = dict()
        while True:
            line = (await reader.readline()).strip()
            if not line:
                break
            k, v = line.decode().split(':', 1)
            hdr[k.lower()] = v.strip()

        value = b'\n'
        if 'get' == method:
            status, seq, value = await get_value(key)

        elif method in ('put', 'post'):
            value = await reader.read(int(hdr['content-length']))
            x, seq = await paxos_propose(key, value, hdr.get('x_seq', 0))
            value = x.encode()

        writer.write('200 OK\nX_SEQ: {}\n'.format(seq).encode())
        writer.write('CONTENT-LENGTH: {}\n\n'.format(len(value)).encode())
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
        read_log=read_log,
        read_key=read_key,
        read_value=read_value,
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


async def paxos_propose(key, value, version):
    quorum = int(len(ARGS.servers)/2) + 1

    # Get the next seq to be used
    responses = await rpc({s: dict(action='read_stats') for s in ARGS.servers})
    if len(responses) < quorum:
        return 'NoSeqQuorum', 0

    seq = max([v['seq'] for v in responses.values()])

    # We use current timestamp as the paxos seq number
    ts = int(time.time()*10**6)

    # Paxos - Promise Phase
    responses = await rpc({s: dict(action='promise', seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoPromiseQuorum', 0

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    accepted = 0
    proposal_kv = (key, value, version)
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted'] > accepted:
            accepted = res['accepted']
            proposal_kv = (res['key'], res['value'], 0)

    # Paxos - Accept Phase
    responses = await rpc({s: dict(action='accept', seq=seq, promised=ts,
                                   key=proposal_kv[0], value=proposal_kv[1],
                                   version=proposal_kv[2])
                           for s in responses})
    if len(responses) < quorum:
        return 'NoAcceptQuorum', 0

    # Paxos - Learn Phase
    responses = await rpc({s: dict(action='learn', seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoLearnQuorum', 0

    return ('ok', seq) if 0 == accepted else ('NotOurProposal', 0)


async def timeout():
    timeout = int(time.time()) % min(ARGS.timeout, 3600)
    log('will exit after sec(%d)', timeout)
    await asyncio.sleep(timeout)
    log('exiting after sec(%d)', timeout)


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
        DB.execute('''create table if not exists log(
            seq      unsigned integer primary key,
            promised unsigned integer,
            accepted unsigned integer,
            key      text,
            value    blob)''')
        DB.execute('create index if not exists log_key on log(key)')

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

    if ARGS.file or ARGS.value:
        if ARGS.file:
            with open(ARGS.file, 'rb') as fd:
                value = fd.read()
        else:
            value = ARGS.value.encode()

        func = paxos_propose(ARGS.key, value, ARGS.version)
    else:
        func = get_value(ARGS.key)

    print(asyncio.get_event_loop().run_until_complete(func))
