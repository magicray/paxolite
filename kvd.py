import json
import time
import sqlite3
import asyncio
import argparse
from logging import critical as log


MAX_SEQ = 99999999999999


class SQLite():
    def __init__(self, db):
        self.conn = sqlite3.connect(db + '.sqlite3')
        self.conn.execute('''create table if not exists paxos_kv(
            log_seq     unsigned integer primary key,
            promise_seq unsigned integer,
            accept_seq  unsigned integer,
            data_key    text,
            data_value  blob)''')
        self.conn.execute('create index if not exists i1 on paxos_kv(data_key)')

        self.commit = self.conn.commit
        self.execute = self.conn.execute

    def __del__(self):
        self.conn.close()


def get_next_log_seq(db):
    row = db.execute('''select log_seq, promise_seq from paxos_kv
                        order by log_seq desc limit 1''').fetchone()

    if row:
        return row[0]+1 if row[1] == MAX_SEQ else row[0]

    return 1


def gather_info(req):
    req['status'] = 'ok'
    req['next_log_seq'] = get_next_log_seq(SQLite(req['db']))

    return req


def read_row(req):
    db = SQLite(req['db'])

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()

    req['status'] = 'NotFound'

    if row:
        req.update(dict(
            status='ok',
            promise_seq=row[1], accept_seq=row[2],
            key=row[3], value=row[4]))

    return req


def insert_row(req):
    db = SQLite(req['db'])

    #if req['log_seq'] != get_next_log_seq(db):
    #    req['status'] = 'InvalidLogSeq'
    #    return req

    db.execute('delete from paxos_kv where log_seq=?',
               (req['log_seq'],))
    db.execute('insert into paxos_kv values(?,?,?,?,?)',
               (req['log_seq'], MAX_SEQ, MAX_SEQ, req['key'], req['value']))
    db.commit()

    req['status'] = 'ok'
    return req


def paxos_promise(req):
    db = SQLite(req['db'])

    # This log_seq would create a hole in the log. Terminate.
    if req['log_seq'] != get_next_log_seq(db):
        req['status'] = 'InvalidLogSeq'
        return req

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()
    # Insert a new row
    if not row:
        req['status'] = 'ok'
        req['accepted_seq'] = 0
        db.execute('insert into paxos_kv(log_seq, promise_seq) values(?, ?)',
                   (req['log_seq'], req['promise_seq']))
        db.commit()
        return req

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promise_seq'] <= row[1]:
        req.update(dict(
            status='OldPromiseSeq',
            existing_promise_seq=row[1]))

        return req

    # Our promise_seq is largest seen so far for this log_seq.
    # Update promise_seq and return current accepted values
    db.execute('update paxos_kv set promise_seq=? where log_seq=?',
               (req['promise_seq'], req['log_seq']))
    db.commit()

    req.update(dict(
        status='ok', existing_promise_seq=row[1],
        accept_seq=row[2] if row[2] else 0,
        key=row[3], value=row[4]))

    return req


def paxos_accept(req):
    db = SQLite(req['db'])

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # We did not participate in the promise phase. Terminate.
    # This is stricter than what paxos asks. Less code/test is better.
    # A new entry is created only in the promise phase.
    if not row:
        req['status'] = 'NotFound'
        return req

    # Though paxos allows to accept if our promise_seq is bigger, we reject.
    # We accept only if we participated in promise phase.
    # Less coding and testing.
    # This is not a violation of paxos as any node can reject/fail for
    # any reason, any time. Protocol still works correctly.
    if req['promise_seq'] != row[1]:
        req['status'] = 'PromiseSeqMismatch'
        return req

    # All good. Our promise_seq is same as in the db.
    db.execute('''update paxos_kv
                  set accept_seq=?, data_key=?, data_value=?
                  where log_seq=?
               ''', (req['promise_seq'], req['key'],
                     req.pop('value'), req['log_seq']))

    db.commit()

    req['status'] = 'ok'
    return req


def paxos_learn(req):
    db = SQLite(req['db'])

    row = db.execute('''select data_key from paxos_kv
                        where log_seq=? and promise_seq=? and accept_seq=?
                     ''', (req['log_seq'], req['promise_seq'],
                           req['promise_seq'])).fetchone()
    if not row:
        req['status'] = 'NotFound'
        return req

    db.execute('delete from paxos_kv where log_seq<? and data_key=?',
               (req['log_seq'], row[0]))

    db.execute('''update paxos_kv set
                  promise_seq=?, accept_seq=?
                  where log_seq=?
               ''', (MAX_SEQ, MAX_SEQ, req['log_seq']))

    db.commit()

    req['status'] = 'ok'
    return req


async def single_rpc(server, req):
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

    value = res.pop('value', None)
    if value:
        res['value'] = await reader.read(value)

    writer.close()
    res['__server__'] = server
    return res


async def rpc(server_req_map):
    tasks = [single_rpc(k, v) for k, v in server_req_map.items()]

    responses = dict()
    for res in await asyncio.gather(*tasks, return_exceptions=True):
        if type(res) is dict and 'ok' == res['status']:
            responses[res.pop('__server__')] = res

    return responses


async def paxos_propose(servers, db, key, value):
    quorum = int(len(servers)/2) + 1

    # Get the best log_seq to be used
    min_seq = 2*60
    min_srv = None
    log_seq = 0
    log_srv = None

    responses = await rpc({s: dict(action='info', db=db) for s in servers})
    if len(responses) < quorum:
        return 'NoQuorum'

    for srv, res in responses.items():
        if res['next_log_seq'] < min_seq:
            min_srv = srv
            min_seq = res['next_log_seq']

        if res['next_log_seq'] > log_seq:
            log_srv = srv
            log_seq = res['next_log_seq']

    # Sync all the servers before doing a new write.
    for seq in range(min_seq, log_seq):
        res = await single_rpc(log_srv, dict(action='read', log_seq=seq,
                                             db=db))

        if type(res) is not dict:
            break

        if 'NotFound' == res['status']:
             continue

        if 'ok' != res['status']:
             break

        if MAX_SEQ != res['promise_seq'] or MAX_SEQ != res['accept_seq']:
            log('This should not happen(%s)', res)
            break

        res = await single_rpc(min_srv, dict(action='insert', log_seq=seq,
                                             db=db, key=res['key'],
                                             value=res['value']))

        if type(res) is not dict or 'ok' != res['status']:
            break

    # We use current timestamp as the paxos seq number
    promise_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # Paxos - Promise Phase
    accepted_seq = 0
    proposal_value = (key, value)
    responses = await rpc({s: dict(action='promise',
                                   db=db, log_seq=log_seq,
                                   promise_seq=promise_seq)
                           for s in responses})
    if len(res) < quorum:
        return 'NoQuorum'

    for res in responses.values():
        if res['accepted_seq'] > accepted_seq:
            accepted_seq = res['accepted_seq']
            proposal_value = (res['key'], res['value'])

    # Paxos - Accept Phase
    responses = await rpc({s: dict(action='accept',
                                   db=db, log_seq=log_seq,
                                   promise_seq=promise_seq,
                                   key=proposal_value[0],
                                   value=proposal_value[1])
                           for s in responses})
    if len(responses) < quorum:
        return 'NoQuorum'

    # Paxos - Learn Phase
    responses = await rpc({s: dict(action='learn',
                                   db=db, log_seq=log_seq,
                                   promise_seq=promise_seq)
                            for s in responses})
    if len(responses) < quorum:
        return 'NoQuorum'

    # accepted_seq == 0 -> Our proposal was accepted.
    # accepted_seq != 0 -> Succeeded, but not with our proposal
    # Client should retry if return value is False
    return 0 == accepted_seq


async def server(reader, writer):
    req = json.loads(await reader.readline())

    value = req.pop('value', None)
    if value:
        req['value'] = await reader.read(value)

    log('request(%s)', req)

    if 'info' == req['action']:
        res = gather_info(req)
    elif 'promise' == req['action']:
        res = paxos_promise(req)
    elif 'accept' == req['action']:
        res = paxos_accept(req)
    elif 'learn' == req['action']:
        res = paxos_learn(req)
    elif 'read' == req['action']:
        res = read_row(req)
    elif 'insert' == req['action']:
        res = insert_row(req)

    value = res.pop('value', None)
    if value:
        res['value'] = len(value)

    writer.write(json.dumps(res).encode())
    writer.write(b'\n')
    if value:
        writer.write(value)

    await writer.drain()
    writer.close()

    log('response(%s)', res)


def client():
    servers = [(s.split(':')[0], int(s.split(':')[1]))
               for s in ARGS.servers.split(',')]

    log(servers)
    asyncio.get_event_loop().run_until_complete(
        paxos_propose(servers, ARGS.db, ARGS.key, ARGS.value.encode()))


if __name__ == '__main__':
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--db', dest='db', default='default')
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--key', dest='key')
    ARGS.add_argument('--file', dest='file')
    ARGS.add_argument('--value', dest='value')
    ARGS.add_argument('--maxsize', dest='maxsize', type=int, default=1024*1024)
    ARGS.add_argument('--servers', dest='servers',
        default='localhost:5000,localhost:5001,localhost:5002')
    ARGS = ARGS.parse_args()

    if ARGS.port:
        asyncio.ensure_future(asyncio.start_server(server, '', ARGS.port))
        log('listening on port(%s)', ARGS.port)
        asyncio.get_event_loop().run_forever()
    else:
        client()
