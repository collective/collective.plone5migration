# -*- coding: utf-8 -*-

# Check exported data in ArangoDB for HTML mess

import os
import json
import argparse
import furl
import tqdm
import uuid
import bs4
from arango import ArangoClient


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", default="ugent", help="ArangoDB database")
    parser.add_argument(
        "-c", "--collection", default="import", help="ArangoDB collection"
    )
    parser.add_argument(
        "-url",
        "--connection-url",
        default="http://localhost:8529",
        help="ArangoDB connection URL",
    )
    parser.add_argument("-u", "--username", default="root", help="ArangoDB username")
    parser.add_argument("-p", "--password", default="", help="ArangoDB password")
    args = parser.parse_args()
    print(f"connection={args.connection_url}")
    print(f"username={args.username}")
    print(f"database={args.database}")
    print(f"collection={args.collection}")

    f = furl.furl(args.connection_url)
    client = ArangoClient(protocol=f.scheme, host=f.host, port=8529)

    db = client.db(args.database, username=args.username, password=args.password)

    query = """
        FOR doc in import
           FILTER doc._type in ['Document', 'Vacancy', 'News Item', 'Event', 'PhdDefense', 'LibraryDocument']
           return doc
    """

    print('Fetching data')
    result = db.aql.execute(query)
    print('Got data')
    for i, r in enumerate(result):
        if i % 1000 == 0:
            print(i)

        for name in ('text', 'toptext', 'bottomtext'):
            if name in r:
                text = r[name]
                soup = bs4.BeautifulSoup(text, 'html.parser')
                try:
                    str(soup)
                except Exception as e:
                    print(f"HTML error in {r['_path']}, field={name} ({e})")


if __name__ == "__main__":
    main()
