# -*- coding: utf-8 -*-

# Read a single document JSON dataset given by path

import json
import os
import pprint
import argparse
import yaml
import attrdict

from .migration_import import Migrator


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="configuration file for migration (YAML format)",
        default="migration.yml"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        dest="output_filename",
        help="Write object JSON to file",
        required=False,
    )
    parser.add_argument(
        "path", help="Path of object in ArangoDB (e.g. /plone_portal/path/to/some.pdf)"
    )
    args = parser.parse_args()

    yaml_fn = os.path.abspath(args.config)

    if not os.path.exists(yaml_fn):
        raise IOError(f"Migration configuration {yaml_fn} not found")

    with open(yaml_fn) as fp:
        config = attrdict.AttrDict(yaml.load(fp, Loader=yaml.FullLoader))

    migrator = Migrator(config, args)
    short_data = migrator._object_by_path(args.path)
    data = migrator._object_by_key(short_data["_key"])
    data = dict((k, data[k]) for k in sorted(data.keys()))

    if args.output_filename:
        with open(args.output_filename, "w") as fp:
            json.dump(data, fp, sort_keys=True, indent=4)
        print(f"Output written to {args.output_filename}")
    else:
        pprint.pprint(data)


if __name__ == "__main__":
    main()
