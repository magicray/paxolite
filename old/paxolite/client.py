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
    log('server%s %s', server, tmp)

    res['server'] = server
    return res


async def rpc(server_req_map):
    tasks = [_rpc(k, v) for k, v in server_req_map.items()]

    responses = dict()
    for res in await asyncio.gather(*tasks, return_exceptions=True):
        if type(res) is dict and 'ok' == res['status']:
            responses[res['server']] = res

    return responses


class Client():
    def __init__(self, servers):
        self.servers = servers

    async def get(self, db, key, existing_version=0):
        quorum = int(len(self.servers)/2) + 1

        responses = await rpc({s: dict(action='read_stats', db=db)
                               for s in self.servers})

        if len(responses) < quorum:
            return 'NoQuorum', 0, b''

        # From the most updated servers, we want to pick in the random order
        seq = max([v['seq'] for v in responses.values()])
        srvrs = [(hashlib.md5(str(time.time()*10**9).encode()).digest(), s)
                 for s in [k for k, v in responses.items() if v['seq'] == seq]]

        # Some servers are out of sync. Tell them about it.
        await rpc({s: dict(action='sync', db=db)
                   for s in set(self.servers)-set([s for _, s in srvrs])})

        # Fetch data from the most updated servers
        # Servers are picked in random order for load balancing
        for _, s in sorted(srvrs):
            res = await rpc({s: dict(action='read_kv', db=db, key=key)})
            res = res[s]

            if 0 == res['version']:
                return 'notfound', 0, b''

            if existing_version == res['version']:
                return 'ok', res['version'], b''

            return 'ok', res['version'], res['value']

    async def put(self, db, key_version_value_list):
        value = pickle.dumps(key_version_value_list)

        for s in self.servers:
            try:
                r = await _rpc(s, dict(action='propose', db=db, value=value))

                if 'ok' == r['status']:
                    return r

                await asyncio.sleep(10 + time.time() % 10)
            except Exception:
                continue
