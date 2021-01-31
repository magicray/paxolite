import json
import time
import sqlite3
import asyncio
import argparse
from logging import critical as log


MAX_SEQ = 99999999999999


def db_connect(db):
    conn = sqlite3.connect(db + '.sqlite3')
    conn.execute('''create table if not exists paxos_kv(
        log_seq     unsigned integer primary key,
        promise_seq unsigned integer,
        accept_seq  unsigned integer,
        data_key    text,
        data_value  blob)''')
    conn.execute('create index if not exists i1 on paxos_kv(data_key)')

    return conn


def gather_info(req):
    db = db_connect(req['db'])

    row = db.execute('''select log_seq, promise_seq from paxos_kv
                        order by log_seq desc limit 1''').fetchone()

    next_log_seq = 1

    if row:
        next_log_seq = row[0]+1 if row[1] == MAX_SEQ else row[0]

    req.update(dict(status='ok', next_log_seq=next_log_seq))

    return req


def paxos_promise(req):
    db = db_connect(req['db'])

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # Insert a new row
    if not row:
        req['status'] = 'ok'
        db.execute('insert into paxos_kv(log_seq, promise_seq) values(?, ?)',
                   (req['log_seq'], req['promise_seq']))
        db.commit()
        return req

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promise_seq'] <= row[1]:
        req.update(dict(
            status='OutdatedPromiseSeq',
            existing_promise_seq=row[1]))

        return req

    # Our promise_seq is largest seen so far for this log_seq.
    # Update promise_seq and return current accepted values
    db.execute('update paxos_kv set promise_seq=? where log_seq=?',
               (req['promise_seq'], req['log_seq']))
    db.commit()

    req.update(dict(
        status='ok',
        existing_promise_seq=row[1], accept_seq=row[2],
        key=row[3], value=row[4]))

    return req


def paxos_accept(req):
    db = db_connect(req['db'])

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # We did not participate in the promise phase. Terminate.
    # This is stricter than what paxos asks. Less code/test we like
    # A new entry is created only in the promise phase. Easy to reason.
    if not row:
        req['status'] = 'NotFound'
        return req

    # This is a different flow as promise_seq does not match. Terminate.
    # This is stricter than what paxos says. We accept only if we promised.
    # Less code and easy to reason. And not a violation of paxos protocol.
    if req['promise_seq'] != row[1]:
        req['status'] = 'PromiseSeqMismatch'
        return req

    db.execute('''update paxos_kv
                  set accept_seq=?, data_key=?, data_value=?
                  where log_seq=?
               ''', (req['promise_seq'], req['key'],
                     req.pop('value'), req['log_seq']))

    db.commit()

    req['status'] = 'ok'
    return req


def paxos_learn(req):
    db = db_connect(req['db'])

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


async def rpc(server, req):
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
    return res


async def multi_rpc(server_req_map):
    tasks = [rpc(k, v) for k, v in server_req_map.items()]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def paxos_propose(key, value):
    promise_seq = int(time.strftime('%Y%m%d%H%M%S'))

    db = 'default'
    servers = [('localhost', 5000), ('localhost', 5001), ('localhost', 5002)]
    quorum = int(len(servers)/2) + 1

    count = 0
    log_seq = 1
    req_map = {s: dict(action='info', db=db) for s in servers}
    for res in await multi_rpc(req_map):
        if type(res) is not dict or 'ok' != res['status']:
            continue

        print(res)
        count += 1

        if res['next_log_seq'] > log_seq:
            log_seq = res['next_log_seq']

    if count < quorum:
        log('NoQuorum(info)')
        return

    count = 0
    accepted_seq = 0
    proposal_value = (key, value)
    req_map = {s: dict(action='promise', db=db,
                       log_seq=log_seq, promise_seq=promise_seq)
               for s in servers}
    for res in await multi_rpc(req_map):
        if type(res) is not dict or 'ok' != res['status']:
            continue

        print(res)
        count += 1

        if res.get('accepted_seq', 0) > accepted_seq:
            proposal_value = (res['key'], res['value'])

    if count < quorum:
        log('NoQuorum(promise)')
        return

    count = 0
    req_map = {s: dict(action='accept', db=db, promise_seq=promise_seq,
                       log_seq=log_seq,
                       key=proposal_value[0], value=proposal_value[1])
               for s in servers}
    for res in await multi_rpc(req_map):
        if type(res) is not dict or 'ok' != res['status']:
            continue

        print(res)
        count += 1

    if count < quorum:
        log('NoQuorum(accept)')
        return

    count = 0
    req_map = {s: dict(action='learn', db=db, promise_seq=promise_seq,
                       log_seq=log_seq)
               for s in servers}
    for res in await multi_rpc(req_map):
        if type(res) is not dict or 'ok' != res['status']:
            continue

        print(res)
        count += 1

    if count < quorum:
        log('NoQuorum(learn)')
        return

    return True


async def server(reader, writer):
    req = json.loads(await reader.readline())
    log('%s', req)

    value = req.pop('value', None)
    if value:
        req['value'] = await reader.read(value)

    if 'info' == req['action']:
        res = gather_info(req)
    elif 'promise' == req['action']:
        res = paxos_promise(req)
    elif 'accept' == req['action']:
        res = paxos_accept(req)
    elif 'learn' == req['action']:
        res = paxos_learn(req)

    value = res.pop('value', None)
    if value:
        res['value'] = len(value)

    writer.write(json.dumps(res).encode())
    writer.write(b'\n')
    if value:
        writer.write(value)

    await writer.drain()
    writer.close()


def client(args):
    asyncio.get_event_loop().run_until_complete(paxos_propose('key', b'value'))


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--maxsize', dest='maxsize', type=int, default=1024*1024)
    args = args.parse_args()

    if args.port:
        asyncio.ensure_future(asyncio.start_server(server, '', args.port))
        log('listening on port(%s)', args.port)
        asyncio.get_event_loop().run_forever()
    else:
        client(args)
