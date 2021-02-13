import time
import json
import sqlite3
import asyncio
import logging
import hashlib
import argparse
from logging import critical as log


DB = None


class cache():
    key = dict()
    value = dict()


def db_init():
    conn = sqlite3.connect('paxolite.db')
    conn.execute('''create table if not exists log(
        seq      unsigned integer primary key,
        promised unsigned integer,
        accepted unsigned integer,
        key      text,
        value    blob)''')
    conn.execute('create index if not exists log_key on log(key)')

    return conn


# Find out the log_seq for the next log entry to be filled.
# It is either the max seq in the db if it is not yet learned,
# or the max seq+1 if the max entry is already learned.
def get_next_seq():
    row = DB.execute('''select seq, promised, accepted from log
                        order by seq desc limit 1''').fetchone()

    if row:
        return row[0]+1 if row[1] is None and row[2] is None else row[0]

    return 1


def read_stats(req):
    req.update(dict(status='ok', seq=get_next_seq()))
    return req


def read_value(req):
    req['status'] = 'NotFound'

    if req['seq'] in cache.value:
        req.update(dict(status='ok', value=cache.value[req['seq']]))

    return req


def read_key(req):
    if req['key'] not in cache.key:
        DB.rollback()

        rows = DB.execute('select * from log where key=?', (req['key'],))

        for row in rows.fetchall():
            if row[1] is None and row[2] is None:
                cache.key[req['key']] = row[0]
                cache.value[row[0]] = row[4]

    req['status'] = 'NotFound'
    if req['key'] in cache.key:
        req.update(dict(status='ok', seq=cache.key[req['key']]))

    return req


def read_log(req):
    DB.rollback()

    req['status'] = 'NotFound'
    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()
    if row:
        req.update(dict(status='ok', promised=row[1], accepted=row[2],
                        key=row[3], value=row[4]))
    return req


def paxos_promise(req):
    # This log_seq would create a hole in the log. Terminate.
    if req['seq'] != get_next_seq():
        req['status'] = 'InvalidSeq'
        return req

    DB.rollback()

    # Insert a new row if it does not already exist for this log seq
    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()
    if not row:
        DB.execute('insert into log(seq, promised, accepted) values(?,?,?)',
                   (req['seq'], 0, 0))

    # We have a row for this log seq for sure
    row = DB.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        req.update(dict(status='SmallerPromiseSeq', existing=row[1]))
        return req

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    DB.execute('update log set promised=? where seq=?',
               (req['promised'], req['seq']))
    DB.commit()

    req.update(dict(status='ok', accepted=row[2], key=row[3], value=row[4]))
    return req


def paxos_accept(req):
    DB.rollback()

    row = DB.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in the promise phase for this log_seq. Terminate.
    # This is stricter than what paxos asks. Inefficient, but simpler code.
    # This is not a violation of paxos as any node can reject/fail for
    # any reason, any time. Protocol still works correctly.
    # A new entry is created only in the promise phase.
    if not row or req['promised'] != row[0]:
        req['status'] = 'PromiseSeqMismatch'
        return req

    # All good. Our promise seq is same as in the db.
    DB.execute('update log set accepted=?, key=?, value=? where seq=?',
               (req['promised'], req['key'], req.pop('value'), req['seq']))
    DB.commit()

    req['status'] = 'ok'
    return req


def paxos_learn(req):
    DB.rollback()

    row = DB.execute('select promised, key from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in promise or accept phase earlier. Reject.
    # Ideally, learning this value is correct and more efficient, still
    # we don't do as we have a separate flow to bring nodes in sync.
    if not row or req['promised'] != row[0]:
        req['status'] = 'PromiseSeqMismatch'
        return req

    DB.execute('delete from log where seq<? and key=?', (req['seq'], row[1]))
    DB.execute('update log set promised=?, accepted=? where seq=?',
               (None, None, req['seq']))
    DB.commit()

    cache.value.pop(cache.key.pop(row[1], 0), None)  # Clear the cache

    req['status'] = 'ok'
    return req


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


async def paxos_propose(key, value):
    quorum = int(len(ARGS.servers)/2) + 1

    # Get the best log_seq to be used
    responses = await rpc({s: dict(action='read_stats') for s in ARGS.servers})
    if len(responses) < quorum:
        return 'NoInfoQuorum'

    seq = max([v['seq'] for v in responses.values()])

    # We use current timestamp as the paxos seq number
    ts = int(time.time())

    # Paxos - Promise Phase
    responses = await rpc({s: dict(action='promise', seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoPromiseQuorum'

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    accepted = 0
    proposal_kv = (key, value)
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted'] > accepted:
            accepted = res['accepted']
            proposal_kv = (res['key'], res['value'])

    # Paxos - Accept Phase
    responses = await rpc({s: dict(action='accept', seq=seq, promised=ts,
                                   key=proposal_kv[0], value=proposal_kv[1])
                           for s in responses})
    if len(responses) < quorum:
        return 'NoAcceptQuorum'

    # Paxos - Learn Phase
    responses = await rpc({s: dict(action='learn', seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoLearnQuorum'

    return 'ok' if 0 == accepted else 'NotOurProposal'


async def server(reader, writer):
    cluster_ip_list = [s[0] for s in ARGS.servers]
    if writer.get_extra_info('peername')[0] not in cluster_ip_list:
        log('client%s not allowed', writer.get_extra_info('peername'))
        return writer.close()

    req = json.loads(await reader.readline())

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


async def get_value(key):
    quorum = int(len(ARGS.servers)/2) + 1

    # Get the best log_seq to be used
    responses = await rpc({s: dict(action='read_key', key=key)
                          for s in ARGS.servers})
    if len(responses) < quorum:
        return 'NoQuorum'

    seq = max([v['seq'] for v in responses.values()])
    srv = [k for k, v in responses.items() if v['seq'] == seq]
    srv = sorted([(hashlib.md5(str(time.time()*10**9).encode()).digest(), s)
                 for s in srv])
    for _, s in srv:
        responses = await rpc({s: dict(action='read_value', seq=seq)})
        for k, v in responses.items():
            return v['value'].decode()


def client():
    if ARGS.value or ARGS.file:
        if ARGS.file:
            with open(ARGS.file, 'rb') as fd:
                value = fd.read()
        else:
            value = ARGS.value.encode()

        return asyncio.get_event_loop().run_until_complete(
            paxos_propose(ARGS.key, value))
    else:
        return asyncio.get_event_loop().run_until_complete(
            get_value(ARGS.key))


async def timeout():
    responses = await rpc({s: dict(action='read_stats') for s in ARGS.servers})

    max_srv, max_seq = None, 0
    for k, v in responses.items():
        if v['seq'] > max_seq:
            max_srv = k
            max_seq = v['seq']

    DB.rollback()
    # Update the servers that has fallen behind
    for seq in range(get_next_seq(), max_seq):
        res = await _rpc(max_srv, dict(action='read_log', seq=seq))

        if type(res) is not dict:
            log('sync failed seq(%d) srv(%s)', seq, max_srv)
            return

        if 'NotFound' == res['status']:
            continue

        if 'ok' != res['status']:
            log('sync failed seq(%d) srv(%s)', seq, max_srv)
            return

        DB.execute('delete from log where seq=?', (seq,))
        DB.execute('delete from log where key=?', (res['key'],))
        DB.execute('insert into log values(?,?,?,?,?)',
                   (seq, None, None, res['key'], res['value']))
        DB.commit()

    timeout = int(time.time()) % ARGS.timeout
    log('will exit after sec(%d)', timeout)
    await asyncio.sleep(timeout)
    log('exiting after sec(%d)', timeout)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--timeout', dest='timeout', type=int, default=30)
    ARGS.add_argument('--key', dest='key',
                      default=time.strftime('%Y%m%d%H%M%S'))
    ARGS.add_argument('--value', dest='value',
                      default=str(int(time.time()*10000000)))
    ARGS.add_argument('--file', dest='file')
    ARGS.add_argument('--servers', dest='servers',
                      default='127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,'
                              '127.0.0.2:5003,127.0.0.1:5004')
    ARGS = ARGS.parse_args()

    ARGS.servers = [(s.split(':')[0].strip(), int(s.split(':')[1]))
                    for s in ARGS.servers.split(',')]
    if ARGS.port:
        DB = db_init()
        asyncio.ensure_future(asyncio.start_server(server, '', ARGS.port))
        log('listening on port(%s)', ARGS.port)
        asyncio.get_event_loop().run_until_complete(
                asyncio.ensure_future(timeout()))
    else:
        print(client())
