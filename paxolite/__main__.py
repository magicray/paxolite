import time
import client
import server
import asyncio
import argparse


ARGS = argparse.ArgumentParser()
ARGS.add_argument('--db', dest='db', default='default')
ARGS.add_argument('--key', dest='key')
ARGS.add_argument('--value', dest='value')
ARGS.add_argument('--version', dest='version', type=int, default=0)

ARGS.add_argument('--port', dest='port', type=int)
ARGS.add_argument('--servers', dest='servers',
                  default='127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,'
                          '127.0.0.2:5003,127.0.0.1:5004')
ARGS = ARGS.parse_args()

servers = [(s.split(':')[0].strip(), int(s.split(':')[1]))
           for s in ARGS.servers.split(',')]

if ARGS.port:
    server.server(ARGS.port, servers)
elif not ARGS.key or ARGS.value:
    if not ARGS.key:
        # This is only for testing - Pick random key and value
        ARGS.key = time.strftime('%H%M')
        ARGS.value = str(time.time()*10**9) * 10**4

    cli = client.Client(servers)
    print(asyncio.get_event_loop().run_until_complete(
        cli.put(ARGS.db, [(ARGS.key, ARGS.version, ARGS.value.encode())])))
else:
    cli = client.Client(servers)
    print(asyncio.get_event_loop().run_until_complete(
        cli.get(ARGS.db, ARGS.key)))
