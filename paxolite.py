import json
import time
import signal
import sqlite3
import asyncio
import logging
import argparse
from logging import critical as log


MAX_SEQ = 99999999999999


class cache():
    log = dict()
    key = dict()


class SQLite():
    conns = dict()

    def __init__(self, db):
        if db not in self.conns:
            conn = sqlite3.connect(db + '.sqlite3')
            conn.execute('''create table if not exists paxos(
                log_seq     unsigned integer primary key,
                promise_seq unsigned integer,
                accept_seq  unsigned integer,
                data_key    text,
                data_value  blob)''')
            conn.execute('create index if not exists i1 on paxos(data_key)')

            self.conns[db] = conn

        self.conn = self.conns[db]
        self.conn.rollback()

        self.commit = self.conn.commit
        self.execute = self.conn.execute


def get_next_log_seq(db):
    row = db.execute('''select log_seq, promise_seq from paxos
                        order by log_seq desc limit 1''').fetchone()

    if row:
        return row[0]+1 if row[1] == MAX_SEQ else row[0]

    return 1


def read_info(req):
    req['status'] = 'ok'
    req['next_log_seq'] = get_next_log_seq(SQLite(req['db']))

    return req


def read_key(req):
    if req['key'] not in cache.key:
        db = SQLite(req['db'])

        rows = db.execute('select * from paxos where data_key=?',
                          (req['key'],)).fetchall()

        assert(len(rows) <= 2)

        for row in rows:
            if MAX_SEQ == row[1] and MAX_SEQ == row[2]:
                cache.key[req['key']] = row[0]
                cache.log[row[0]] = (row[3], row[4])

    req['status'] = 'NotFound'
    if req['key'] in cache.key:
        req.update(dict(status='ok', log_seq=cache.key[req['key']]))

    return req


def read_row(req):
    if req['log_seq'] in cache.log:
        key, value = cache.log[req['log_seq']]

        req.update(dict(status='ok', promise_seq=MAX_SEQ, accept_seq=MAX_SEQ,
                        key=key, value=value))
        return req

    db = SQLite(req['db'])

    row = db.execute('select * from paxos where log_seq=?',
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

    next_log_seq = get_next_log_seq(db)
    if req['log_seq'] < next_log_seq:
        req['status'] = 'InvalidLogSeq'
        return req

    if not req['key'] or not req['value']:
        log('BUG - This should never happen(%s)', req)
        req['status'] = 'InvalidKeyValue'
        return req

    db.execute('''delete from paxos
                  where log_seq=? and (promise_seq!=? or accept_seq!=?)
               ''', (next_log_seq, MAX_SEQ, MAX_SEQ))
    db.execute('delete from paxos where data_key=?', (req['key'],))
    db.execute('delete from paxos where log_seq=?', (req['log_seq'],))
    db.execute('insert into paxos values(?,?,?,?,?)', (req['log_seq'],
               MAX_SEQ, MAX_SEQ, req['key'], req['value']))
    db.commit()

    # Clear the cache
    cache.log.pop(cache.key.pop(req['key'], 0), None)

    req['status'] = 'ok'
    return req


def paxos_promise(req):
    db = SQLite(req['db'])

    # This log_seq would create a hole in the log. Terminate.
    if req['log_seq'] != get_next_log_seq(db):
        req['status'] = 'InvalidLogSeq'
        return req

    row = db.execute('select * from paxos where log_seq=?',
                     (req['log_seq'],)).fetchone()
    # Insert a new row
    if not row:
        req['status'] = 'ok'
        req['accepted_seq'] = 0
        db.execute('insert into paxos(log_seq, promise_seq) values(?, ?)',
                   (req['log_seq'], req['promise_seq']))
        db.commit()
        return req

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promise_seq'] <= row[1]:
        req.update(dict(
            status='OldPromiseSeq',
            old_promise_seq=row[1]))

        return req

    # Our promise_seq is largest seen so far for this log_seq.
    # Update promise_seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    db.execute('update paxos set promise_seq=? where log_seq=?',
               (req['promise_seq'], req['log_seq']))
    db.commit()

    req.update(dict(
        status='ok', old_promise_seq=row[1],
        accepted_seq=row[2] if row[2] else 0,
        key=row[3], value=row[4]))

    return req


def paxos_accept(req):
    db = SQLite(req['db'])

    row = db.execute('select * from paxos where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # We did not participate in the promise phase for this log_seq. Terminate.
    # This is stricter than what paxos asks. Inefficient, but simpler code.
    # This is not a violation of paxos as any node can reject/fail for
    # any reason, any time. Protocol still works correctly.
    # A new entry is created only in the promise phase.
    if not row:
        req['status'] = 'NotFound'
        return req

    # Though paxos allows to accept if our promise_seq is bigger,
    # we reject unless these values are same.
    # This is stricter than what paxos asks. Inefficient, but simpler code.
    # We accept only if we participated in promise phase.
    # This is not a violation of paxos as any node can reject/fail for
    # any reason, any time. Protocol still works correctly.
    if req['promise_seq'] != row[1]:
        req['status'] = 'PromiseSeqMismatch'
        return req

    if not req['key'] or not req['value']:
        log('BUG - This should never happen(%s)', req)
        req['status'] = 'InvalidKeyValue'
        return req

    # All good. Our promise_seq is same as in the db.
    db.execute('''update paxos set accept_seq=?, data_key=?, data_value=?
                  where log_seq=?
               ''', (req['promise_seq'], req['key'],
                     req.pop('value'), req['log_seq']))

    db.commit()

    req['status'] = 'ok'
    return req


def paxos_learn(req):
    db = SQLite(req['db'])

    row = db.execute('''select data_key from paxos
                        where log_seq=? and promise_seq=? and accept_seq=?
                     ''', (req['log_seq'], req['promise_seq'],
                           req['promise_seq'])).fetchone()
    # We did not participate in promise or accept phase earlier. Reject.
    # Ideally, learning this value is correct and more efficient, still
    # we don't do as we have a separate flow to bring nodes in sync.
    if not row:
        req['status'] = 'NotFound'
        return req

    db.execute('delete from paxos where log_seq<? and data_key=?',
               (req['log_seq'], row[0]))

    db.execute('update paxos set promise_seq=?, accept_seq=? where log_seq=?',
               (MAX_SEQ, MAX_SEQ, req['log_seq']))

    db.commit()

    # Clear the cache
    cache.log.pop(cache.key.pop(row[0], 0), None)

    req['status'] = 'ok'
    return req


async def _rpc(server, req):
    reader, writer = await asyncio.open_connection(server[0], server[1])

    value = req.pop('value', None)
    if value:
        req['value'] = len(value)

    # log('server%s request(%s)', server, req)
    writer.write(json.dumps(req).encode())
    writer.write(b'\n')
    if value:
        writer.write(value)

    await writer.drain()

    res = json.loads((await reader.readline()).decode())

    log('server%s response(%s)', server, res)
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


async def paxos_propose(servers, db, key, value):
    quorum = int(len(servers)/2) + 1

    # Get the best log_seq to be used
    responses = await rpc({s: dict(action='info', db=db) for s in servers})
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
        res = await _rpc(log_srv, dict(action='read', log_seq=seq, db=db))

        if type(res) is not dict:
            break

        if 'NotFound' == res['status']:
            continue

        if 'ok' != res['status']:
            break

        if MAX_SEQ != res['promise_seq'] or MAX_SEQ != res['accept_seq']:
            log('This should not happen(%s)', res)
            break

        res = await rpc({min_srv: dict(action='insert', log_seq=seq,
                                       db=db, key=res['key'],
                                       value=res['value'])})
        if not res:
            break

    # We use current timestamp as the paxos seq number
    promise_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # Paxos - Promise Phase
    responses = await rpc({s: dict(action='promise',
                                   db=db, log_seq=log_seq,
                                   promise_seq=promise_seq)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoPromiseQuorum'

    accepted_seq = 0
    proposal_value = (key, value)
    for res in responses.values():
        # This is the KEY step in paxos protocol
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
        return 'NoAcceptQuorum'

    # Paxos - Learn Phase
    responses = await rpc({s: dict(action='learn',
                                   db=db, log_seq=log_seq,
                                   promise_seq=promise_seq)
                           for s in responses})
    if len(responses) < quorum:
        return 'NoLearnQuorum'

    # Our proposal was accepted
    if 0 == accepted_seq:
        return 'ok'

    # Successful, but not with our proposal
    return 'NotOurProposal'


async def server(reader, writer):
    if writer.get_extra_info('peername')[0] not in ARGS.clients:
        log('client%s not allowed', writer.get_extra_info('peername'))
        return writer.close()

    req = json.loads(await reader.readline())

    value = req.pop('value', None)
    if value:
        req['value'] = await reader.read(value)

    # log('client%s request(%s)', writer.get_extra_info('peername'), req)

    if 'info' == req['action']:
        res = read_info(req)
    elif 'promise' == req['action']:
        res = paxos_promise(req)
    elif 'accept' == req['action']:
        res = paxos_accept(req)
    elif 'learn' == req['action']:
        res = paxos_learn(req)
    elif 'read' == req['action']:
        res = read_row(req)
    elif 'key' == req['action']:
        res = read_key(req)
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

    log('client%s response(%s)', writer.get_extra_info('peername'), res)


async def read_value(servers, db, key):
    quorum = int(len(servers)/2) + 1

    # Get the best log_seq to be used
    responses = await rpc({s: dict(action='key', db=db, key=key)
                          for s in servers})
    if len(responses) < quorum:
        return 'NoQuorum'

    log_seq, log_srv = 0, None
    for srv, res in responses.items():
        res = await rpc({srv: dict(action='read', db=db,
                        log_seq=res['log_seq'])})
        if res and res[srv]['log_seq'] > log_seq:
            log_srv = srv

    if log_srv:
        return res[log_srv]['value'].decode()


def client():
    servers = [(s.split(':')[0], int(s.split(':')[1]))
               for s in ARGS.servers.split(',')]

    if ARGS.value or ARGS.file:
        if ARGS.file:
            with open(ARGS.file, 'rb') as fd:
                value = fd.read()
        else:
            value = ARGS.value.encode()

        return asyncio.get_event_loop().run_until_complete(
            paxos_propose(servers, ARGS.db, ARGS.key, value))
    else:
        return asyncio.get_event_loop().run_until_complete(
            read_value(servers, ARGS.db, ARGS.key))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--db', dest='db', default='paxos')
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--timeout', dest='timeout', type=int, default=30)
    ARGS.add_argument('--key', dest='key',
                      default=time.strftime('%Y%m%d%H%M%S'))
    ARGS.add_argument('--value', dest='value',
                      default=str(time.time()*1000000))
    ARGS.add_argument('--file', dest='file')
    ARGS.add_argument('--clients', dest='clients', default='127.0.0.1')
    ARGS.add_argument('--servers', dest='servers',
                      default='localhost:5000,localhost:5001,localhost:5002')
    ARGS = ARGS.parse_args()

    if ARGS.port:
        signal.alarm(int(time.time()*1000) % ARGS.timeout)
        ARGS.clients = [ip.strip() for ip in ARGS.clients.split(',')]
        asyncio.ensure_future(asyncio.start_server(server, '', ARGS.port))
        log('listening on port(%s)', ARGS.port)
        asyncio.get_event_loop().run_forever()
    else:
        print(client())
