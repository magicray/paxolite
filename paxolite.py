import json
import time
import sqlite3
import asyncio
import logging
import hashlib
import argparse
from logging import critical as log


DB = None
MAX_SEQ = 99999999999999


class cache():
    key = dict()
    value = dict()


def db_init():
    conn = sqlite3.connect('paxolite.db')
    conn.execute('''create table if not exists paxos(
        log_seq     unsigned integer primary key,
        promise_seq unsigned integer,
        accept_seq  unsigned integer,
        data_key    text,
        data_value  blob)''')
    conn.execute('create index if not exists i1 on paxos(data_key)')

    return conn


# Find out the log_seq for the next log entry to be filled.
# It is either the max log_seq in the db if it is not yet learned,
# or the max log_seq+1 if the max entry is already learned.
def get_next_log_seq():
    row = DB.execute('''select log_seq, promise_seq from paxos
                        order by log_seq desc limit 1''').fetchone()

    if row:
        return row[0]+1 if row[1] == MAX_SEQ else row[0]

    return 1


def read_stats(req):
    req.update(dict(status='ok', next_log_seq=get_next_log_seq()))
    return req


def read_value(req):
    req['status'] = 'NotFound'

    if req['log_seq'] in cache.value:
        req.update(dict(status='ok', value=cache.value[req['log_seq']]))

    return req


def read_key(req):
    if req['key'] not in cache.key:
        DB.rollback()

        rows = DB.execute('select * from paxos where data_key=?',
                          (req['key'],)).fetchall()

        for row in rows:
            if MAX_SEQ == row[1] and MAX_SEQ == row[2]:
                cache.key[req['key']] = row[0]
                cache.value[row[0]] = row[4]

    req['status'] = 'NotFound'
    if req['key'] in cache.key:
        req.update(dict(status='ok', log_seq=cache.key[req['key']]))

    return req


def read_log(req):
    DB.rollback()

    req['status'] = 'NotFound'
    row = DB.execute('select * from paxos where log_seq=?',
                     (req['log_seq'],)).fetchone()
    if row:
        req.update(dict(status='ok', promise_seq=row[1], accept_seq=row[2],
                        key=row[3], value=row[4]))
    return req


def paxos_promise(req):
    # This log_seq would create a hole in the log. Terminate.
    if req['log_seq'] != get_next_log_seq():
        req['status'] = 'InvalidLogSeq'
        return req

    DB.rollback()

    row = DB.execute('select * from paxos where log_seq=?',
                     (req['log_seq'],)).fetchone()
    # Insert a new row
    if not row:
        DB.execute('insert into paxos(log_seq, promise_seq) values(?, ?)',
                   (req['log_seq'], req['promise_seq']))
        DB.commit()

        req.update(dict(status='ok', accepted_seq=0))
        return req

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promise_seq'] <= row[1]:
        req.update(dict(status='InvalidPromiseSeq', old_promise_seq=row[1]))
        return req

    # Our promise_seq is largest seen so far for this log_seq.
    # Update promise_seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    DB.execute('update paxos set promise_seq=? where log_seq=?',
               (req['promise_seq'], req['log_seq']))
    DB.commit()

    req.update(dict(status='ok', accepted_seq=row[2] if row[2] else 0,
                    key=row[3], value=row[4]))
    return req


def paxos_accept(req):
    DB.rollback()

    row = DB.execute('select promise_seq from paxos where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # We did not participate in the promise phase for this log_seq. Terminate.
    # This is stricter than what paxos asks. Inefficient, but simpler code.
    # This is not a violation of paxos as any node can reject/fail for
    # any reason, any time. Protocol still works correctly.
    # A new entry is created only in the promise phase.
    if not row or req['promise_seq'] != row[0]:
        req['status'] = 'SeqMismatch'
        return req

    # All good. Our promise_seq is same as in the db.
    DB.execute('''update paxos set accept_seq=?, data_key=?, data_value=?
                  where log_seq=?
               ''', (req['promise_seq'], req['key'],
                     req.pop('value'), req['log_seq']))
    DB.commit()

    req['status'] = 'ok'
    return req


def paxos_learn(req):
    DB.rollback()

    # This is for sync
    if req.get('key', None) and req.get('value', None):
        DB.execute('delete from paxos where log_seq=?', (get_next_log_seq(),))

        row = DB.execute('select log_seq from paxos where log_seq=?',
                         (req['log_seq'],)).fetchone()
        if row:
            req['status'] = 'EntryExists'
            return req

        DB.execute('delete from paxos where data_key=?', (req['key'],))
        DB.execute('insert into paxos values(?,?,?,?,?)', (req['log_seq'],
                   MAX_SEQ, MAX_SEQ, req['key'], req['value']))

        cache.value.pop(cache.key.pop(req['key'], 0), None)  # Clear the cache

    # This is paxos learn phase.
    else:
        row = DB.execute('''select promise_seq, data_key from paxos
                            where log_seq=?
                         ''', (req['log_seq'],)).fetchone()
        # We did not participate in promise or accept phase earlier. Reject.
        # Ideally, learning this value is correct and more efficient, still
        # we don't do as we have a separate flow to bring nodes in sync.
        if not row or req['promise_seq'] != row[0]:
            req['status'] = 'SeqMismatch'
            return req

        DB.execute('delete from paxos where log_seq<? and data_key=?',
                   (req['log_seq'], row[1]))

        DB.execute('''update paxos set promise_seq=?, accept_seq=?
                      where log_seq=?
                   ''', (MAX_SEQ, MAX_SEQ, req['log_seq']))

        cache.value.pop(cache.key.pop(row[1], 0), None)  # Clear the cache

    DB.commit()

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

    min_srv, min_seq = None, 2**64   # This server is lagging behind
    log_srv, log_seq = None, 0       # This server has the most data

    for srv, res in responses.items():
        if res['next_log_seq'] < min_seq:
            min_srv = srv
            min_seq = res['next_log_seq']

        if res['next_log_seq'] > log_seq:
            log_srv = srv
            log_seq = res['next_log_seq']

    # Update the servers that has fallen behind
    for seq in range(min_seq, log_seq):
        res = await _rpc(log_srv, dict(action='read_log', log_seq=seq))

        if type(res) is not dict:
            break

        if 'NotFound' == res['status']:
            continue

        if 'ok' != res['status']:
            break

        res = await rpc({min_srv: dict(action='learn', log_seq=seq,
                                       key=res['key'], value=res['value'])})
        if not res:
            break

    # We use current timestamp as the paxos seq number
    promise_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # Paxos - Promise Phase
    responses = await rpc({s: dict(action='promise', log_seq=log_seq,
                                   promise_seq=promise_seq)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoPromiseQuorum'

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    accepted_seq = 0
    proposal_kv = (key, value)
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted_seq'] > accepted_seq:
            accepted_seq = res['accepted_seq']
            proposal_kv = (res['key'], res['value'])

    # Paxos - Accept Phase
    responses = await rpc({s: dict(action='accept', log_seq=log_seq,
                                   promise_seq=promise_seq,
                                   key=proposal_kv[0],
                                   value=proposal_kv[1])
                           for s in responses})
    if len(responses) < quorum:
        return 'NoAcceptQuorum'

    # Paxos - Learn Phase
    responses = await rpc({s: dict(action='learn', log_seq=log_seq,
                                   promise_seq=promise_seq)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoLearnQuorum'

    return 'ok' if 0 == accepted_seq else 'NotOurProposal'


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

    seq = max([r['log_seq'] for r in responses.values()])
    srv = [k for k, v in responses.items() if v['log_seq'] == seq]
    srv = sorted([(hashlib.md5(str(time.time()*10**9).encode()).digest(), s)
                 for s in srv])
    for _, s in srv:
        responses = await rpc({s: dict(action='read_value', log_seq=seq)})
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
