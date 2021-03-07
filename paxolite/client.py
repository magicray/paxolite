import time
import pickle
import asyncio
import hashlib
from logging import critical as log


async def _rpc(server, req):
    reader, writer = await asyncio.open_connection(server[0], server[1])

    writer.write(pickle.dumps(req))
    await writer.drain()

    res = pickle.loads((await reader.read()))
    writer.close()

    tmp = res.copy()
    tmp.pop('value', None)
    log('server%s rpc(%s)', server, tmp)

    res['__server__'] = server
    return res


async def rpc(server_req_map):
    tasks = [_rpc(k, v) for k, v in server_req_map.items()]

    responses = dict()
    for res in await asyncio.gather(*tasks, return_exceptions=True):
        if type(res) is dict and 'ok' == res['status']:
            responses[res.pop('__server__')] = res

    return responses


async def paxos_propose(servers, value):
    quorum = int(len(servers)/2) + 1

    # We use current timestamp as the paxos seq number
    ts = int(time.time()/30) * 30

    # Promise Phase
    responses = await rpc({s: dict(action='promise', promised=ts)
                           for s in servers})

    if len(responses) < quorum:
        return 'NoPromiseQuorum', 0

    seq = max([v['seq'] for v in responses.values()])

    responses = {k: v for k, v in responses.items() if v['seq'] == seq}

    if len(responses) < quorum:
        return 'NoPromiseQuorum', 0

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
                           for s in servers})
    if len(responses) < quorum:
        return 'NoLearnQuorum', 0

    return ('ok', seq) if 0 == proposal[0] else ('ProposalConflict', 0)


class Client():
    def __init__(self, servers):
        self.servers = servers

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
        for i in range(50):
            result = await paxos_propose(
                self.servers, pickle.dumps(key_version_value_list))

            if 'ok' == result[0]:
                return result

            await asyncio.sleep(1)

    def sync(self, async_callable):
        return asyncio.get_event_loop().run_until_complete(async_callable)
