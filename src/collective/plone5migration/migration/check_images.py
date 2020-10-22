# -*- coding: utf-8 -*-

# Check all images before migration import


import sys
import os
import json
import time
import pprint
import traceback
import argparse
import tqdm
import yaml
import attrdict
import multiprocessing
import base64
import io
import PIL.Image

from .migration_import import Migrator
from .logger import get_logger

LOG = get_logger("image_check.log", stdout=False)
LOG.info("Starting image check")


class ImageChecker(Migrator):
    def check_images(self):
        query = f"""
            FOR doc in  {self.collection_name}
                FILTER doc._type == 'Image'
                RETURN {{path: doc._path,
                        _key: doc._key
                       }}
                """

        LOG.info("Querying database for images")

        result = self._query_aql(query)
        result = [r for r in result]

        LOG.info(f"Found {len(result)} images")
        pool = multiprocessing.Pool(processes=self.args.number_processes)
        with pool as p:
            result = list(
                tqdm.tqdm(p.imap(self._check_image, result), total=len(result))
            )

    def _check_image(self, row):

        key = row["_key"]
        json_data = self._object_by_key(key)

        try:
            image_field = json_data["_datafield_image"]
        except KeyError as e:
            LOG.error(f"ERROR: {row['path']}: {e}")
            return

        image_data = image_field["data"]
        image_data = base64.b64decode(image_data)
        img = None
        try:
            img = PIL.Image.open(io.BytesIO(image_data))
            #            LOG.error(f"OK: {row['path']}")
        except OSError as e:
            LOG.error(f"ERROR: {row['path']}: {e}")
        except PIL.Image.DecompressionBombError as e:
            LOG.error(f"ERROR: {row['path']}: {e}")
        except UnboundLocalError as e:
            # https://github.com/python-pillow/Pillow/issues/3769
            LOG.error(f"ERROR: {row['path']}: {e}")

        if img and img.format == 'TIFF':
            ct = image_field['content_type']
            if ct != 'image/tiff': 
                 LOG.error(f"ERROR: {row['path']}: TIFF disguised as {ct}")

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", help="configuration file for migration (YAML format)", required=True
    )
    parser.add_argument(
        "-p",
        "--processes",
        dest="number_processes",
        default=1,
        help="Number of processes (parallel checks)",
        type=int,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose mode (timing)"
    )
    args = parser.parse_args()

    if args.verbose:
        global VERBOSE
        VERBOSE = True

    yaml_fn = os.path.abspath(args.config)
    LOG.info(f"Reading {yaml_fn}")

    if not os.path.exists(yaml_fn):
        raise IOError(f"Migration configuration {yaml_fn} not found")

    with open(yaml_fn) as fp:
        config = attrdict.AttrDict(yaml.load(fp, Loader=yaml.FullLoader))
    pprint.pprint(config)

    image_checker = ImageChecker(config, args)
    image_checker.check_images()


if __name__ == "__main__":
    main()
