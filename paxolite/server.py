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

logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')


class Database():
    def __init__(self, name=None):
        self.conn = sqlite3.connect(
            'paxolite.{}.sqlite3'.format(name) if name else 'paxolite.sqlite3')

        self.commit = self.conn.commit
        self.execute = self.conn.execute
        self.rollback = self.conn.rollback

        self.execute('''create table if not exists kv(
            key      text primary key,
            version  unsigned integer,
            value    blob)''')
        self.execute('''create table if not exists log(
            seq      unsigned integer primary key,
            promised unsigned integer,
            accepted unsigned integer,
            checksum text,
            value    blob)''')


# Utility methods
def response(x, status, **kwargs):
    x.update(kwargs)
    x['status'] = status
    return x


# Find out the log seq for the next log entry to be filled.
def get_next_seq(db):
    row = db.execute('''select seq, promised from log
                        order by seq desc limit 1''').fetchone()

    # It is either the max seq in the db if it is not yet learned,
    # or the max seq+1 if the max entry is already learned.
    if row:
        return row[0]+1 if row[1] is None else row[0]

    # Nothing yet in the db, lets start our log with seq number 1
    return 1


def read_stats(req):
    return response(req, 'ok', seq=get_next_seq(Database()))


def read_log(req):
    db = Database()

    row = db.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    if row and row[1] is None and row[2] is None:
        return response(req, 'ok', checksum=row[3], value=row[4])

    return response(req, 'ok')


def read_kv(req):
    db = Database()

    row = db.execute('select * from kv where key=?', (req['key'],)).fetchone()
    if row:
        return response(req, 'ok', version=row[1], value=row[2])

    return response(req, 'ok', version=0)


def check_kv_table(db, blob):
    for key, version, value in pickle.loads(blob):
        if version:
            row = db.execute('select version from kv where key=?',
                             (key,)).fetchone()
            if row[0] != version:
                return False

    return True


def update_kv_table(db, seq, blob):
    for key, version, value in pickle.loads(blob):
        db.execute('delete from kv where key=?', (key,))
        if value:
            db.execute('insert into kv values(?,?,?)', (key, seq, value))

    return True


def paxos_promise(req):
    db = Database()

    seq = get_next_seq(db)

    # Insert a new row if it does not already exis
    row = db.execute('select * from log where seq=?', (seq,)).fetchone()
    if not row:
        # Make a new entry in the log
        db.execute('insert into log(seq, promised, accepted) values(?,?,?)',
                   (seq, 0, 0))
        row = db.execute('select * from log where seq=?', (seq,)).fetchone()

    # Our promise_seq is not bigger than the existing one. Terminate now.
    if req['promised'] <= row[1]:
        return response(req, 'InvalidSeq', seq=seq)

    # Our promise seq is largest seen so far for this log seq.
    # Update promise seq and return current accepted values.
    # This is the KEY step in paxos protocol.
    db.execute('update log set promised=? where seq=?', (req['promised'], seq))
    db.commit()

    return response(req, 'ok', seq=seq, accepted=row[2], value=row[4])


def paxos_accept(req):
    db = Database()

    row = db.execute('select promised from log where seq=?',
                     (req['seq'],)).fetchone()

    # We did not participate in the promise phase. Reject.
    if not row or req['promised'] != row[0]:
        return response(req, 'InvalidPromise')

    if check_kv_table(db, req['value']) is not True:
        return response(req, 'TxnConflict')

    # All good. Accept this proposal. If a quorum reaches this step, this value
    # is learned. This seq -> value mapping is now permanent.
    # Proposer would detect that a quorum reached this stage and would in the
    # next learn phase, record this fact by setting promised/accepted as NULL.
    db.execute('update log set accepted=?, value=? where seq=?',
               (req['promised'], req.pop('value'), req['seq']))
    db.commit()

    return response(req, 'ok')


def compute_checksum(db, seq, value):
    chksum = db.execute('select checksum from log where seq=?',
                        (seq-1,)).fetchone()
    chksum = chksum[0] if chksum and chksum[0] else ''

    checksum = hashlib.md5()
    checksum.update(chksum.encode())
    checksum.update(value)
    return checksum.hexdigest()


def paxos_learn(req):
    db = Database()

    row = db.execute('select * from log where seq=?', (req['seq'],)).fetchone()

    # We did not participate in promise/accept phase. Reject.
    if not row or req['promised'] != row[1]:
        return response(req, 'InvalidPromise')

    checksum = compute_checksum(db, req['seq'], row[4])
    db.execute('''update log
                  set promised=null, accepted=null, checksum=?
                  where seq=?
               ''', (checksum, req['seq']))
    update_kv_table(db, req['seq'], row[4])
    db.commit()

    return response(req, 'ok')


async def paxos_propose(servers, value):
    quorum = int(len(servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time()/30) * 30

    # Promise Phase
    responses = await rpc({s: dict(action='promise', promised=ts)
                           for s in servers})

    if len(responses) < quorum:
        return dict(status='NoPromiseQuorum', seq=0)

    seq = max([v['seq'] for v in responses.values()])

    responses = {k: v for k, v in responses.items() if v['seq'] == seq}

    if len(responses) < quorum:
        return dict(status='NoPromiseQuorum', seq=0)

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
        return dict(status='NoAcceptQuorum', seq=0)

    # Learn Phase
    responses = await rpc({s: dict(action='learn', seq=seq, promised=ts)
                           for s in servers})
    if len(responses) < quorum:
        return dict(status='NoLearnQuorum', seq=0)

    return dict(
        status='ok' if 0 == proposal[0] else 'ProposalConflict',
        seq=seq if 0 == proposal[0] else 0)


def sync(peers):
    db = Database()

    # Update the servers that has fallen behind
    for peer in peers:
        db.rollback()
        seq = get_next_seq(db)

        while True:
            db.rollback()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer[0], peer[1]))

            sock.sendall(pickle.dumps(dict(action='read_log', seq=seq)))
            res = pickle.load(sock.makefile(mode='b'))
            sock.close()

            if 'value' not in res:
                break

            checksum = compute_checksum(db, seq, res['value'])
            assert(checksum == res['checksum'])

            db.execute('''delete from log
                          where seq=? and promised is not null
                       ''', (seq,))
            db.execute('insert into log values(?,null,null,?,?)',
                       (seq, checksum, res['value']))
            update_kv_table(db, seq, res['value'])
            db.commit()

            seq += 1


def dict2str(src):
    dst = src.copy()
    dst.pop('value', None)
    return dst


def server(port, peers):
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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

    if 'propose' == req['action']:
        res = asyncio.get_event_loop().run_until_complete(
            paxos_propose(peers, req['value']))
    else:
        res = dict(
            read_kv=read_kv,
            read_log=read_log,
            read_stats=read_stats,
            learn=paxos_learn,
            accept=paxos_accept,
            promise=paxos_promise,
        )[req['action']](req)

    conn.sendall(pickle.dumps(res))
    conn.close()

    log('client%s res(%s)', addr, dict2str(res))

    if 'learn' == req['action'] and 'ok' != res['status']:
        sync(peers)
