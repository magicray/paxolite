import time
import client
import server
import argparse


ARGS = argparse.ArgumentParser()

ARGS.add_argument('--key', dest='key')
ARGS.add_argument('--file', dest='file')
ARGS.add_argument('--value', dest='value')
ARGS.add_argument('--version', dest='version', type=int, default=0)

ARGS.add_argument('--port', dest='port', type=int)
ARGS.add_argument('--servers', dest='servers',
                  default='127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002,'
                          '127.0.0.2:5003,127.0.0.1:5004')

ARGS = ARGS.parse_args()

ARGS.servers = [(s.split(':')[0].strip(), int(s.split(':')[1]))
                for s in ARGS.servers.split(',')]

# Start the server
if ARGS.port:
    exit(server.server(ARGS.port, ARGS.servers))

# Client - CLI
if not ARGS.key:
    # This is only for testing - Pick random key and value
    ARGS.key = time.strftime('%y%m%d.%H%M%S.') + str(time.time()*10**6)
    ARGS.value = ARGS.key

cli = client.Client(ARGS.servers)
if ARGS.file or ARGS.value:
    if ARGS.file:
        with open(ARGS.file, 'rb') as fd:
            value = fd.read()
    else:
        value = ARGS.value.encode()

    print(cli.sync(cli.put([(ARGS.key, ARGS.version, value)])))
else:
    print(cli.sync(cli.get(ARGS.key)))
