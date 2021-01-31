import os
import json
import time
import signal
import socket
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


def paxos_promise(req):
    db = db_connect(req['db'])

    row = db.execute('''select log_seq, promise_seq
                        from paxos_kv
                        order by log_seq desc limit 1
                    ''').fetchone()

    # Either we are just getting started and db is empty.
    # Or, the row for largest log_seq has been already learned,
    # and we create a new row for the next log entry
    if not row or row[1] == MAX_SEQ:
        req.update(dict(
            status='ok',
            log_seq=1 if not row else row[0]+1))

        db.execute('insert into paxos_kv(log_seq, promise_seq) values(?, ?)',
                   (req['log_seq'], req['promise_seq']))
        db.commit()
        return req

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promise_seq'] <= row[1]:
        req.update(dict(
            status='OutdatedPromiseSeq',
            log_seq=row[0],
            existing_promise_seq=row[1]))

        return req

    # Our promise_seq is largest seen so far for this log_seq.
    # Update promise_seq and return current accepted values
    db.execute('update paxos_kv set promise_seq=? where log_seq=?',
               (req['promise_seq'], row[0]))

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (row[0],)).fetchone()
    db.commit()

    req.update(dict(
        status='ok', log_seq=row[0],
        promise_seq=row[1], accept_seq=row[2],
        key=row[3], value=row[4]))

    return req


def paxos_accept(req):
    db = db_connect(req['db'])

    row = db.execute('select * from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()

    # Our promise_seq is less than the existing seq. Terminate.
    if row and req['promise_seq'] < row[1]:
        req.update(dict(status='OutdatedPromiseSeq'))
        return req

    # No entry for this log_seq found. No conflicts. Accept.
    if not row:
        row = (log_seq, 0, 0, None, None)
        db.execute('insert into paxos_kv values(?,?,?,?,?)',
                   (req['log_seq'], req['promise_seq'], req['promise_seq'],
                    req['key'], value))

    # Our promise_seq is greater or equal to existing value. Accept.
    else:
        db.execute('''update paxos_kv
                      set promise_seq=?, accept_seq=?, data_key=?, data_value=?
                      where log_seq=?
                   ''', (req['promise_seq'], req['promise_seq'], req['key'],
                         req['value'], req['log_seq']))

    db.commit()

    req.pop('value')
    req.update(dict(status='ok',
        existing_promise_seq=row[1], existing_accept_seq=row[2],
        existing_key=row[3]))
    return req


def paxos_learn(req):
    db = db_connect(req['db'])

    key = db.execute('select data_key from paxos_kv where log_seq=?',
                     (req['log_seq'],)).fetchone()[0]
    db.execute('delete from paxos_kv where data_key=? and log_seq<?',
               (key, req['log_seq']))
    db.execute('''update paxos_kv set promise_seq=?, accept_seq=?
                  where log_seq=? and promise_seq=? and accept_seq=? and
                        data_key is not null and data_value is not null
               ''', (MAX_SEQ, MAX_SEQ, req['log_seq'],
                     req['promise_seq'], req['promise_seq']))
    db.commit()

    req['status'] ='ok'
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
    log_seq = set()
    req_map = {s: dict(action='promise', db=db, promise_seq=promise_seq)
               for s in servers}
    for res in await multi_rpc(req_map):
        print(res)
        if type(res) is not dict:
            continue

        if 'ok' == res['status']:
            count += 1

        log_seq.add(res['log_seq'])

    if count < quorum or len(log_seq) != 1:
        return

    log_seq = log_seq.pop()

    count = 0
    req_map = {s: dict(action='accept', db=db, promise_seq=promise_seq,
                       log_seq=log_seq, key=key, value=value)
               for s in servers}
    for res in await multi_rpc(req_map):
        print(res)
        if type(res) is not dict:
            continue

        if 'ok' == res['status']:
            count += 1

    if count < quorum:
        return

    count = 0
    req_map = {s: dict(action='learn', db=db, promise_seq=promise_seq,
                       log_seq=log_seq)
               for s in servers}
    for res in await multi_rpc(req_map):
        print(res)
        if type(res) is not dict:
            continue

        if 'ok' == res['status']:
            count += 1

    if count < quorum:
        return

    return True


async def server(reader, writer):
    req = json.loads(await reader.readline())
    log('%s', req)

    value = req.pop('value', None)
    if value:
        req['value'] = await reader.read(value)

    if 'promise' == req['action']:
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
    args.add_argument('--maxsize', dest='maxsize', type=int, default=10*1024*1024)
    args = args.parse_args()

    if args.port:
        asyncio.ensure_future(asyncio.start_server(server, '', args.port))
        log('listening on port(%s)', args.port)
        asyncio.get_event_loop().run_forever()
    else:
        client(args)
