#!/usr/bin/env python
import asyncio
import websockets
from argparse import ArgumentParser
import ssl
import json
from pprint import pprint
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from realestate_com_au.objects.property import mapper_registry, Property, PropertyEvent, PropertyTrend


def get_parser():
    parser = ArgumentParser(
        description="start a websocket server, capture REA objects into database")
    parser.add_argument("--bind", type=str, default="127.0.0.1",
                        help="websocket bind address")
    parser.add_argument("--port", type=int, default=42069,
                        help="websocket bind port")
    parser.add_argument("--pem", default="RootCA.pem",
                        help="ssl certificate path")
    parser.add_argument("--key", default="RootCA.key",
                        help="ssl private key path")
    parser.add_argument("--sql_url", type=str,
                        help="url for sqlalchemy.create_engine")
    parser.add_argument("--debug", default=False,
                        action="store_true", help="debug sqlalchemy")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    Base = mapper_registry.generate_base()

    engine = create_engine(
        args.sql_url,
        echo=args.debug,
        future=True
    )

    Base.metadata.create_all(engine)

    # create handler for each connection
    async def handler(websocket, path):
        data = await websocket.recv()
        data = json.loads(data)
        pprint(data)
        with Session(engine) as session:
            incoming = Property.from_json(data)
            p = session.query(Property).filter(
                Property.actual_id == incoming.actual_id).one_or_none()
            if not p:
                p = session.query(Property).filter(
                    Property.address == incoming.address
                ).one_or_none()
            if p:
                p.merge(incoming)
                print(f"MERGING MATCHED PROPERTY!!!\n{p} ({p.actual_id})\n")
            else:
                p = incoming
                print(f"MERGING NEW PROPERTY!!!\n{p}\n")
            p = session.merge(p)
            print(f"actual id is now {p.actual_id}")
            session.commit()
            session.flush()
            # session.refresh(p)

    # generate cert with
    # openssl req -x509 -nodes -new -sha256 -days 390 -newkey rsa:2048 -keyout "RootCA.key" -out "RootCA.pem" -subj "/C=de/CN=localhost.local"
    # openssl x509 -outform pem -in "RootCA.pem" -out "RootCA.crt"
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(args.pem, keyfile=args.key)
    print("starting server")
    start_server = websockets.serve(
        handler, args.bind, args.port, ssl=ssl_context)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
