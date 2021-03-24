import os
import time
import pickle
import socket
import signal
import sqlite3
import logging
import hashlib
import asyncio
from client import rpc
from logging import critical as log


db = None


# Find out the log seq for the next log entry to be filled.
def get_next_seq():
    row = db.execute('''select seq, promised from log
                        order by seq desc limit 1''').fetchone()

    # It is either the max seq in the db if it is not yet learned,
    # or the max seq+1 if the max entry is already learned.
    if row:
        return row[0]+1 if row[1] is None else row[0]

    # Nothing yet in the db, lets start our log with seq number 1
    return 1


def read_stats(req):
    return dict(status='ok', seq=get_next_seq())


def read_log(req):
    row = db.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    if row and row[1] is None and row[2] is None:
        return dict(status='ok', checksum=row[3], value=row[4])

    return dict(status='ok')


def read_kv(req):
    row = db.execute('select * from kv where key=?', (req['key'],)).fetchone()
    if row:
        return dict(status='ok', version=row[1], value=row[2])

    return dict(status='ok', version=0)


def paxos_promise(req):
    db.execute('insert into log values(0,null,null,null,null)')
    seq = get_next_seq()

    # Insert a new row if it does not already exist
    row = db.execute('select * from log where seq=?', (seq,)).fetchone()
    if not row:
        # Make a new entry in the log
        db.execute('insert into log(seq, promised, accepted) values(?,?,?)',
                   (seq, 0, 0))
        row = db.execute('select * from log where seq=?', (seq,)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        return dict(status='InvalidSeq', seq=seq)

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    db.execute('update log set promised=? where seq=?', (req['promised'], seq))

    db.execute('delete from log where seq < ?', (seq-10000,))
    db.execute('delete from log where seq=0')
    db.commit()

    return dict(status='ok', seq=seq, accepted=row[2], value=row[4])


def paxos_accept(req):
    db.execute('insert into log values(0,null,null,null,null)')

    row = db.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in the promise phase. Reject.
    if not row or req['promised'] != row[0]:
        return dict(status='InvalidPromise')

    # Optimistic lock check. Ensure no one already updated the key
    for key, version, value in pickle.loads(req['value']):
        if version:
            row = db.execute('select version from kv where key=?',
                             (key,)).fetchone()
            if row[0] != version:
                return dict(status='TxnConflict')

    # All good. Accept this proposal. If a quorum reaches this step, this value
    # is learned. This seq -> value mapping is now permanent.
    # Proposer would detect that a quorum reached this stage and would in the
    # next learn phase, record this fact by setting promised/accepted as NULL.
    db.execute('update log set accepted=?, value=? where seq=?',
               (req['promised'], req.pop('value'), req['seq']))

    db.execute('delete from log where seq=0')
    db.commit()

    return dict(status='ok')


def compute_checksum(seq, value):
    chksum = db.execute('select checksum from log where seq=?',
                        (seq-1,)).fetchone()
    chksum = chksum[0] if chksum and chksum[0] else ''

    checksum = hashlib.md5()
    checksum.update(chksum.encode())
    checksum.update(value)
    return checksum.hexdigest()


def update_kv_table(seq, blob):
    for key, version, value in pickle.loads(blob):
        db.execute('delete from kv where key=?', (key,))
        if value:
            db.execute('insert into kv values(?,?,?)', (key, seq, value))


def paxos_learn(req):
    db.execute('insert into log values(0,null,null,null,null)')

    row = db.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    # We did not participate in promise/accept phase. Reject.
    if not row or req['promised'] != row[1]:
        return dict(status='InvalidLearn')

    checksum = compute_checksum(req['seq'], row[4])
    db.execute('''update log
                  set promised=null, accepted=null, checksum=?
                  where seq=?
               ''', (checksum, req['seq']))
    update_kv_table(req['seq'], row[4])

    db.execute('delete from log where seq=0')
    db.commit()

    return dict(status='ok')


async def paxos_propose(db, servers, value):
    quorum = int(len(servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time())

    # Promise Phase
    responses = await rpc({s: dict(action='promise', db=db, promised=ts)
                           for s in servers})
    if len(responses) < quorum:
        return dict(status='NoPromiseQuorum', seq=0)

    seq = max([v['seq'] for v in responses.values()])

    responses = {k: v for k, v in responses.items() if v['seq'] == seq}

    if len(responses) < quorum:
        await rpc({s: dict(action='sync', db=db)
                   for s in set(servers)-set(responses)})

        return dict(status='SyncNeeded', seq=0)

    # Find the best proposal that was already accepted in some previous round
    # We must discard our proposal and use that. KEY paxos step.
    proposal = (0, value)
    for res in responses.values():
        # This is the KEY step in paxos protocol
        if res['accepted'] > proposal[0] and res['seq'] == seq:
            proposal = (res['accepted'], res['value'])

    # Accept Phase
    responses = await rpc({s: dict(action='accept', db=db, seq=seq,
                                   promised=ts, value=proposal[1])
                           for s in responses})
    if len(responses) < quorum:
        return dict(status='NoAcceptQuorum', seq=0)

    # Learn Phase
    responses = await rpc({s: dict(action='learn', db=db, seq=seq, promised=ts)
                           for s in responses})
    if len(responses) < quorum:
        await rpc({s: dict(action='sync', db=db)
                   for s in set(servers)-set(responses)})

        return dict(status='NoLearnQuorum', seq=0)

    return dict(
        status='ok' if 0 == proposal[0] else 'ProposalConflict',
        seq=seq if 0 == proposal[0] else 0)


async def sync(db_name, peers):
    db.execute('insert into log values(0,null,null,null,null)')

    responses = await rpc({s: dict(action='read_stats', db=db_name)
                           for s in peers})

    seq = max([v['seq'] for k, v in responses.items()])
    peer = [k for k, v in responses.items() if v['seq'] == seq][0]

    seq = get_next_seq()
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((peer[0], peer[1]))

        sock.sendall(pickle.dumps(dict(action='read_log',
                                       db=db_name, seq=seq)))
        res = pickle.load(sock.makefile(mode='b'))
        sock.close()

        if 'value' not in res:
            break

        checksum = compute_checksum(seq, res['value'])
        assert(checksum == res['checksum'])

        db.execute('''delete from log
                      where seq=? and promised is not null
                   ''', (seq,))
        db.execute('insert into log values(?,null,null,?,?)',
                   (seq, checksum, res['value']))
        update_kv_table(seq, res['value'])

        seq += 1

    db.execute('delete from log where seq=0')
    db.commit()


def server(port, peers):
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', port))
    sock.listen()
    log('server started on port(%d)', port)

    while True:
        conn, addr = sock.accept()

        if os.fork():
            conn.close()
        else:
            sock.close()
            break

    req = pickle.load(conn.makefile(mode='rb'))

    global db
    db = sqlite3.connect('paxolite.' + req['db'] + '.sqlite3')

    if 'propose' == req['action']:
        res = asyncio.get_event_loop().run_until_complete(
            paxos_propose(req['db'], peers, req['value']))
        req.pop('value')

    elif req['action'] in ('promise', 'accept', 'learn'):
        if addr[0] in [ip for ip, port in peers]:
            db.execute('''create table if not exists kv(
                key      text primary key,
                version  unsigned integer,
                value    blob)''')
            db.execute('''create table if not exists log(
                seq      unsigned integer primary key,
                promised unsigned integer,
                accepted unsigned integer,
                checksum text,
                value    blob)''')

            res = dict(
                learn=paxos_learn,
                accept=paxos_accept,
                promise=paxos_promise,
            )[req['action']](req)

    elif req['action'] in ('read_kv', 'read_log', 'read_stats'):
        res = dict(
            read_kv=read_kv,
            read_log=read_log,
            read_stats=read_stats,
        )[req['action']](req)

    elif 'sync' == req['action']:
        res = dict(status='ok')

    else:
        res = dict(status='NotAllowed')

    req.update(res)
    conn.sendall(pickle.dumps(req))
    conn.close()

    req.pop('value', None)
    log('client%s %s', addr, req)

    if 'sync' == req['action']:
        asyncio.get_event_loop().run_until_complete(sync(req['db'], peers))
