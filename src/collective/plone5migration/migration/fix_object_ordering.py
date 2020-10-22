# -*- coding: utf-8 -*-

# Post-migration of object positions in partent

import json
import os
import pprint
import argparse
import yaml
import attrdict
import requests
from requests.auth import HTTPBasicAuth

from .migration_import import Migrator

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="configuration file for migration (YAML format)",
        default="migration.yml"
    )
    args = parser.parse_args()

    yaml_fn = os.path.abspath(args.config)

    if not os.path.exists(yaml_fn):
        raise IOError(f"Migration configuration {yaml_fn} not found")

    with open(yaml_fn) as fp:
        config = attrdict.AttrDict(yaml.load(fp, Loader=yaml.FullLoader))

    migrator = Migrator(config, args)

    # read all folders first
    query = """
        FOR doc in import
            filter doc._type == 'Folder'
            limit 10000000
            RETURN  {path: doc._path, position: doc._gopip}
    """

    print('Reading folders ')
    result = migrator._query_aql(query)
    print('Got data')

    folders = list()
    for i, f in enumerate(result):
        folders.append(f)

    folders.append(dict(path="/plone_portal", position=0))

    for i, r in enumerate(folders):

        folder_path = r['path']
        print(i, folder_path)

        query = """
            FOR doc in import
                filter doc._parent_path == '%s'
                RETURN  {path: doc._path, position: doc._gopip}
        """ % folder_path

        result = migrator._query_aql(query)
        positions = list()
        for j, r2 in enumerate(result):
            positions.append(r2)

        if not positions:
            continue

        # sort positions and reset positions to index 0
        positions = sorted(positions, key=lambda x: x['position'])
        positions = [dict(path=d['path'], position=i) for i, d in enumerate(positions)]

        json_headers = {"accept": "application/json", "content-type": "application/json"}
        auth = HTTPBasicAuth(config.plone.username, config.plone.password)

        url = f"{config.plone.url}/{config.site.id}/@@set-positions-in-parent"
        http_result = requests.post(
            url,
            auth=auth,
            headers=json_headers,
            json=positions)

    print('DONE')

if __name__ == "__main__":
    main()
