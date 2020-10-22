# -*- coding: utf-8 -*-

# This scripts imports a JSON export made with `collective.jsonify` into
# ArangoDB (database: collective, collection: portal by default)

import os
import json
import argparse
import furl
import tqdm
import uuid
from arango import ArangoClient


import_dir = "/home/ajung/content_plone_portal_2019-05-13-11-47-28/"


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", default="collective", help="ArangoDB database")
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
    parser.add_argument(
        "-i",
        "--import-directory",
        default=import_dir,
        help="Import directory with JSON files",
    )
    parser.add_argument(
        "-x", "--drop-collection", action="store_true", help="Drop collection"
    )

    args = parser.parse_args()
    print(f"connection={args.connection_url}")
    print(f"username={args.username}")
    print(f"database={args.database}")
    print(f"collection={args.collection}")
    print(f"import directory={args.import_directory}")

    f = furl.furl(args.connection_url)
    client = ArangoClient(hosts=args.connection_url)
    db = client.db(args.database, username=args.username, password=args.password)

    if not db.has_collection(args.collection):
        db.create_collection(name=args.collection)
    else:
        if not args.drop_collection:
            raise RuntimeError("collection already exists - drop it first")
        print("truncating existing collection")
        db.delete_collection(args.collection)
        print("truncating existing collection...DONE")
        db.create_collection(name=args.collection)

    collection = db[args.collection]
    collection.add_hash_index(fields=["_paths_all[*]"])
    collection.add_hash_index(fields=["_directly_provided[*]"])
    collection.add_hash_index(fields=["relatedItems[*]"])
    collection.add_hash_index(fields=["hasRelatedItems"])
    collection.add_hash_index(fields=["_path"])
    collection.add_hash_index(fields=["_parent_path"])
    collection.add_hash_index(fields=["_object_id"])
    collection.add_hash_index(fields=["review_state"])
    collection.add_hash_index(fields=["_type"])
    collection.add_hash_index(fields=["_uid"])
    collection.add_hash_index(fields=["_gopip"])

    files = list()
    for dirname, dirnames, filenames in os.walk(args.import_directory):
        for filename in filenames:
            fn = os.path.join(dirname, filename)
            if fn.endswith(".json"):
                files.append(fn)

    num_files = len(files)
    for i in tqdm.tqdm(range(num_files)):

        fn = files[i]
        with open(fn) as fp:
            try:
                data = json.load(fp)
            except Exception as e:
                print(f"Unable to parse {fn} ({e}")
                continue


        # precalculate all parent paths
        all_paths = list()
        paths2 = data["_path"].split("/")
        paths2 = [p for p in paths2 if p]
        for i in range(1, len(paths2)):
            all_paths.append("/" + "/".join(paths2[:i]))
        relative_path = "/".join(paths2[1:])
        parent_path = '/' + '/'.join(paths2[:-1])
        data['_parent_path'] = parent_path
        data["_paths_all"] = all_paths
        data["_relative_path"] = relative_path
        data["hasRelatedItems"] = len(data.get("relatedItems", ())) > 0
        data["_import_type"] = "content"  # mark imported content as content
        data["_object_id"] = data["_id"]  # original object id
        del data["_id"]  # don't mess up with ArangoDB _id magic
        data["_key"] = str(uuid.uuid4())  # provide our own unique key, some UUID here
        data["_json_filename"] = fn  # JSON import filename

        # save it back to ArangoDB
        collection.insert(data)


if __name__ == "__main__":
    main()
