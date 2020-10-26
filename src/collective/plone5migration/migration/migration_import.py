# -*- coding: utf-8 -*-

# This scripts imports a JSON export made with `collective.jsonify` into
# ArangoDB (database: collection, collection: portal by default)

import re

import base64
import magic
import sys
import os
import json
import time
import pprint
import traceback
import datetime
import itertools
import argparse
import dateparser
from dateutil.parser import parse
import tqdm
import yaml
import furl
import attrdict
import logging
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from arango import ArangoClient

from .pfg import PFGMigrator
from .topic import TopicMigrator
from .yes_no import query_yes_no
from .logger import get_logger


LOG = get_logger("migration.log")


# folderish portal type for which we reconstruct the initial hierarchy in phase
# 1
FOLDERISH_PT = ["Folder", "RichFolder"]

# portal types that are not processed or just ignored because they are
# subobjects e.g. of FormFolder or Collection

IGNORED_TYPES = [
    "Checkbox Field",
    "FormMailerAdapter",
    "FormRichLabelField",
    "FormSaveDataAdapter",
    "FormStringField",
    "FormTextField",
    "FormThanksPage",
    "Page Template" # contained in som FormFolders for debugging
]


# list of ignored or obsolete permissions
IGNORED_PERMISSIONS = ["Change portal events"]


# portal types for which we have actually a migration as primary objects
PROCESSED_TYPES = [
    "Document",
    "News Item",
    "Link",
    "File",
    "Image",
    "RichFolder",
    "FormFolder",
    "Topic",
    "Event",
    "LibraryDocument",
]

# Retrieve vocabularies via plone.restapi
INTROSPECT_VOCABULARIES = [
]

# marker interfaces directly provided by content object (_directly_provided JSON key)
# and supported by the migration
SUPPORTED_MARKER_INTERFACES = [
    "plone.app.layout.navigation.interfaces.INavigationRoot",
]

PARENT_EXISTS_CACHE = dict()

VERBOSE = False

# for start/event of events, we need to substract 4 hours
# due to improper export with improper TZ offset (of 2 hours)
# and a miscalcuation in plone.restapi
OFFSET_HOURS = 4

URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def to_iso8601(s):
    """ Convert format like 2020-08-25T17:00:00+02:00 to ISO8601 """
    dt = parse(s)
    dt = dt - datetime.timedelta(hours=OFFSET_HOURS)
    return dt.isoformat()


def to_ascii(s):
    """ Strip of non-ascii characters (used to cleanup event_url) """
    return s.encode('ascii', errors="ignore").decode()


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class MigrationError(Exception):
    def __init__(self, message, response):
        self.message = message
        self.response = response

    def __str__(self):
        return f"{self.__class__.__name__} {self.response}\n{self.response.text}"


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if "log_time" in kw:
            name = kw.get("log_name", method.__name__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            if VERBOSE:
                LOG.info("%r  %2.2f ms" % (method.__name__, (te - ts) * 1000))
        return result

    return timed


def check_204_response(response):
    if response.status_code == 204 and response.text:
        LOG.info("HTTP 204 response return response with message body")
        import pdb

        pdb.set_trace()


class Migrator:
    """ Migration wrapper """

    def __init__(self, config, args):
        self.config = config  # YAML configutation
        self.args = args  # command line options

        # ArangoDB connection
        arango = self.config.arango
        f = furl.furl(arango.url)
        self.client = ArangoClient(hosts=arango.url)
        self.db = self.client.db(
            arango.database, username=arango.username, password=arango.password
        )
        if not self.db.has_collection(arango.collection):
            raise MigrationError(f'collection "{arango.collection}" does not exist')
        self.collection_name = arango.collection
        self.collection = self.db[arango.collection]
        self._subdepartments = []
        self._all_related_items = dict()  # list of _key values of migrated content
        self._all_imagerefs = dict()  # maps UID of News Item to UID of imageref
        self._deferred_uids = dict() # maps _key to list of fields with UIDs for deferred assignment
        self._deferred_default_pages = dict() # maps _key to default_page
        self._content_with_portlets = (
            dict()
        )  # all processed (_key, resouce_path) attribute from content

        self.requests_session = requests.Session()
        retries = Retry(
            total=6,
            backoff_factor=20,
            status_forcelist=[500, 502, 503, 504],
            method_whitelist=("HEAD", "GET", "POST", "DELETE", "PUT"),
        )
        self.requests_session.mount("http://", HTTPAdapter(max_retries=retries))
        self.requests_session.mount("https://", HTTPAdapter(max_retries=retries))

    @timeit
    def _query_aql(self, query):
        result = self.db.aql.execute(query)
        return result

    @timeit
    def _object_by_key(self, key):
        result = self.collection.get(dict(_key=key))
        return result

    @timeit
    def _object_by_path(self, path):
        query = f"""
            FOR doc in  {self.collection_name}
                FILTER doc._path == '{path}'
                RETURN {{path: doc._path,
                        portal_type: doc._type,
                        id: doc._id,
                        title: doc.title,
                        _key: doc._key
                       }}
                """

        result = self._query_aql(query)
        result = [r for r in result]
        if len(result) == 1:
            return result[0]
        elif len(result) > 1:
            raise ValueError(f'More than one object returned for search by path "{path}"')
        else:
            raise ValueError(f'No object returned for search by path "{path}"')

    def _to_iso8601(self, s):
        """ Convert a date string from collective.jsonify export format to ISO8601 """
        if s in (None, "None"):
            return None
        dt = dateparser.parse(s)
        return dt.isoformat()

    @property
    def _json_headers(self):
        """ Standard JSON headers """
        return {"accept": "application/json", "content-type": "application/json"}

    @property
    def _auth(self):
        """ Credentials for Plone target site """
        return HTTPBasicAuth(self.config.plone.username, self.config.plone.password)

    def read_vocabularies(self, names):
        """ Retrieve vacabularies via plone.restapi """

        self.vocabularies = {}

        for name in names:
            LOG.info(f'reading vocabulary {name}')
            url = f"{self.config.plone.url}/{self.config.site.id}/@vocabularies/{name}?b_size:int=99999999"
            response = self.requests_session.get(
                url, auth=self._auth, headers=self._json_headers
            )
            if response.status_code != 200:
                raise MigrationError(
                    f"DELETE failed for {url}: {response.text}", response=response
                )
            self.vocabularies[name] = dict()
            for d in response.json()['items']:
                self.vocabularies[name][d['token']] = d['title']

    def _check_value_in_vocabulary(self, vocabulary_name, value):
        """ Check if the given `value` (aka a "token") exists in the given remote vocabulary """

        if not vocabulary_name in self.vocabularies:
            raise ValueError(f"No data for vocabulary '{vocabulary_name}'")
        return value in self.vocabularies[vocabulary_name]

    @property
    def site_languages(self):
        try:
            languages = self.config.site.languages
        except (AttributeError, KeyError):
            raise RuntimeError("YAML configuration has no site.languages configuration")
        if not languages:
            raise RuntimeError(
                "YAML configuration has empty site.languages configuration"
            )
        return languages

    def default_language(self):
        return self.site_languages[0]

    @timeit
    def create_plone_site(self):
        LOG.info("creating new plone site")

        url = "{0}/@@recreate-plone-site".format(self.config.plone.url)
        data = {
            "site_id": self.config.site.id,
            "extension_ids": self.config.site.extension_ids,
        }
        response = self.requests_session.post(url, auth=self._auth, json=data)

        if response.status_code != 201:
            raise MigrationError("Site could not be created", response=response)

    @timeit
    def prepare(self):
        """ Prepare migration """

        LOG.info(f"Preparing migration")
        url = f"{self.config.plone.url}/{self.config.site.id}/@@prepare"
        response = self.requests_session.post(
            url, auth=self._auth, headers=self._json_headers
        )
        if response.status_code not in (200,):
            raise MigrationError(
                f"Prepare failed for {url}: {response.text}", response=response
            )


    @timeit
    def fixup (self):
        """ Fixup migration """

        LOG.info(f"Fixup migration")
        url = f"{self.config.plone.url}/{self.config.site.id}/@@fixup"
        response = self.requests_session.post(
            url, auth=self._auth, headers=self._json_headers
        )
        if response.status_code not in (200,):
            raise MigrationError(
                f"Fixup failed for {url}: {response.text}", response=response
            )

    @timeit
    def _delete_resource(self, path):
        """ Remove the given resource by full relative (e.g. /plone_portal/some/resource) """

        LOG.info(f"deleting {path}")
        url = f"{self.config.plone.url}{path}"
        response = self.requests_session.delete(
            url, auth=self._auth, headers=self._json_headers
        )
        check_204_response(response)
        if response.status_code not in (204, 404):
            raise MigrationError(
                f"DELETE failed for {url}: {response.text}", response=response
            )

    @timeit
    def remote_exists(self, path):
        """ Check if the given `path` exists on the remote Plone site """

        url = f"{self.config.plone.url}/{self.config.site.id}/@@remote-exists"
        response = self.requests_session.get(url, auth=self._auth, params=dict(path=path))
        if response.status_code in [200, 204]:
            return True
        elif response.status_code == 404:
            return False
        raise MigrationError(
            f"GET {path} return error code {response.status_code} (expected 200 or 404)",
            response=response,
        )

    @timeit
    def remote_exists_old(self, path):
        """ Check if the given `path` exists on the remote Plone site """

        url = f"{self.config.plone.url}/{self.config.site.id}/{path}"
        response = self.requests_session.head(url, auth=self._auth)
        if response.status_code in (302, 200):
            return True
        elif response.status_code == 404:
            return False
        raise MigrationError(
            f"GET {path} return error code {response.status_code} (expected 200 or 404)",
            response=response,
        )

    @timeit
    def _create_object(self, path, _key):
        """ Create remote content for the given path and the _key data """

        object_data = self._object_by_key(_key)

        post_create_data = {}
        default_page_data = None

        # UIDs must be assigned after full migration
        self._deferred_uids[_key] = dict()

        default_page = object_data.get('_defaultpage')
        if default_page:
            self._deferred_default_pages[_key] = default_page

        # have portlets
        if object_data.get("_portlets"):
            self._content_with_portlets[_key] = path

        # _type is always available
        # _meta_type is available from a particular JSON export version on
        if object_data["_type"] in IGNORED_TYPES:
            LOG.info("IGNORED: {object_data._type}")
            return
        if object_data.get("_meta_type") in IGNORED_TYPES:
            LOG.info("IGNORED: {object_data._meta_type}")
            return

        data = {
            "@type": object_data["_type"],
            "id": object_data["_object_id"],
            "description": object_data.get("description", ""),
            "contributors": object_data.get("contributors", ()),
            "creators": object_data.get("creators", ()),
            "subjects": object_data.get("subject", ()),
            "location": object_data.get("location", ""),
            "exclude_from_nav": object_data.get("excludeFromNav", True),
        }

        # titles could be empty or contain only whitespaces (default to ID)
        title = object_data.get("title", "").strip()  # there are empty titles
        if not title:
            title = object_data["_object_id"]
        data["title"] = title

        # set language (default to first configured site language)
        language = object_data.get("language", self.default_language)
        if language not in self.config.site.languages:
            languages = self.config.site.default_language
        object_data["language"] = language

        related_items = object_data.get("relatedItems", ())
        if related_items:
            self._all_related_items[object_data["_uid"]] = related_items

        effective = self._to_iso8601(object_data.get("effectiveDate"))
        if effective:
            data["effective"] = effective

        expires = self._to_iso8601(object_data.get("expirationDate"))
        if expires:
            data["expires"] = effective

        table_of_contents = object_data.get("tableContents")
        if table_of_contents:
            data["table_of_contents"] = table_of_contents

        if object_data["_type"] == "Document":
            data["text"] = object_data["text"]

            all_subdepartments = self._get_subdepartments()
            subdepartment = object_data.get("subdepartment", "")
            if subdepartment and not subdepartment in all_subdepartments:
                LOG.error(f"Unknown subdepartment {subdepartment} for {path} ")
                subdepartment = ''
            data["subdepartment"] = subdepartment

        elif object_data["_type"] == "Event":
            data["start"] = to_iso8601(object_data["startDate"])
            data["end"] = to_iso8601(object_data["endDate"])
            if object_data['eventUrl']:
                data["event_url"] = to_ascii(object_data["eventUrl"])
            if object_data['contactEmail']:
                data["contact_email"] = object_data["contactEmail"]
            if object_data['contactName']:
                data["contact_name"] = object_data["contactName"]
            if object_data['contactPhone']:
                data["contact_phone"] = object_data["contactPhone"]
            data["attendees"] = object_data["contactPhone"]
            data["text"] = object_data["text"]
            categories = object_data.get('categories', [])

        elif object_data["_type"] == "News Item":
            data["text"] = object_data["text"]

        elif object_data["_type"] == "File":
            try:
                file_data = object_data["_datafield_file"]
            except KeyError:
                LOG.info(
                    f"ERROR: JSON export has no _datafield_file for {path} - SKIPPING"
                )
                return

            data["file"] = {
                "data": file_data["data"],
                "encoding": "base64",
                "content-type": file_data["content_type"],
                "filename": file_data["filename"],
            }

        elif object_data["_type"] == "Image":
            try:
                img_data = object_data["_datafield_image"]
            except KeyError:
                LOG.info(
                    f"ERROR: JSON export has no _datafield_image for {path} - SKIPPING"
                )
                return

            ct = img_data["content_type"]

            # images like BMPs are exported with content_type application/octet-stream which
            # are not properly recognized by plone.restapi
            if not ct.startswith("image/"):
                mime = magic.Magic(mime=True)
                image_data = base64.b64decode(img_data["data"])
                ct = mime.from_buffer(image_data)

            data["image"] = {
                "data": img_data["data"],
                "encoding": "base64",
                "content-type": ct,
                "filename": img_data["filename"],
            }

        elif object_data["_type"] == "Link":
            data["remoteUrl"] = object_data["remoteUrl"]

        elif object_data["_type"] == "Folder":

            directly_provided = object_data.get("_directly_provided", ())
            directly_provided = [
                iface
                for iface in directly_provided
                if iface in SUPPORTED_MARKER_INTERFACES
            ]

            data["navigation_root"] = (
                True if object_data.get("navigation_root") in (True, "True") else False
            )

        elif object_data["_type"] == "RichFolder":
            import pdb; pdb.set_trace()
            data["_type"] = 'Folder'

        elif object_data["_type"] == "FormFolder":
            self._migrate_FormFolder(data, object_data)

        elif object_data["_type"] == "Topic":
            self._migrate_Topic(data, object_data)


        resource_path = "/".join(path.split("/")[:-1])
        #        LOG.info('Creating', resource_path, data)
        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}"


        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(data, cls=CustomJSONEncoder),
        )
        if result.status_code not in (200, 201):
            raise MigrationError(result.text, response=result)


        self._set_owner(path, object_data)
        self._set_layout(path, object_data)
#        self._set_default_page(path, object_data)
        self._set_unavailable(path, object_data)
        self._set_review_state(path, object_data)
        self._set_related_items(path, object_data)
        self._set_local_roles(path, object_data)

        self._set_uid(path, object_data)
        self._set_created_modified(path, object_data)
        # apply folder restrictions after migration because otherwise we can not migrate properly
        #        self._set_allowed_and_addable_types(path, object_data)
        self._set_position_in_parent(path, object_data)
        self._set_marker_interfaces(path, object_data)
        self._set_portlet_blacklist(path, object_data)
        self._set_permissions(path, object_data)

    #        self._set_portlets(path, object_data)

        if post_create_data:
            url2 = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/{data['id']}"
            result2 = self.requests_session.patch(
                url2,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(post_create_data, cls=CustomJSONEncoder),
            )
            if result2.status_code != 204:
                raise MigrationError(result.text, response=result2)

    @timeit
    def _migrate_FormFolder(self, data, object_data):

        data["@type"] = "EasyForm"
        form_prologue = object_data.get("formPrologue")
        if form_prologue:
            data["formPrologue"] = form_prologue
        form_epilogue = object_data.get("formEpilogue")
        if form_epilogue:
            data["formEpilogue"] = form_epilogue

        # query for subobjects by path
        form_path = object_data["_path"]
        query = f"""
            FOR doc in  {self.collection_name}
                FILTER '{form_path}' in doc._paths_all
                SORT doc._gopip
                RETURN {{path: doc._path,
                        portal_type: doc._type,
                        id: doc._id,
                        title: doc.title,
                        _key: doc._key,
                        position_in_parent: doc._gopip
                       }}
                """

        result = self._query_aql(query)
        _keys = [r["_key"] for r in result]

        pfg_migrator = PFGMigrator(
            migration_data=data,  # data for plone.restapi (by reference)
            object_data=object_data,  # exported data of old FormFolder
            child_keys=_keys,  # _key of all child objects
            migrator=self,  # main migrator
        )
        pfg_migrator.migrate()  # updates `data` by reference inside the migrator

    @timeit
    def _migrate_Topic(self, data, object_data):

        data["@type"] = "Collection"

        # query for subobjects by path
        form_path = object_data["_path"]
        query = f"""
            FOR doc in  {self.collection_name}
                FILTER '{form_path}' in doc._paths_all
                SORT doc._gopip
                RETURN {{path: doc._path,
                        portal_type: doc._type,
                        id: doc._id,
                        title: doc.title,
                        _key: doc._key,
                        position_in_parent: doc._gopip
                       }}
                """

        result = self._query_aql(query)
        _keys = [r["_key"] for r in result]

        topic_migrator = TopicMigrator(
            migration_data=data,  # data for plone.restapi (by reference)
            object_data=object_data,  # exported data of old FormFolder
            child_keys=_keys,  # _key of all child objects
            migrator=self,  # main migrator
            log=LOG,
        )
        topic_migrator.migrate()  # updates `data` by reference inside the migrator

    @timeit
    def _set_related_items(self, resource_path, object_data):
        """ Set UID to original UID
        """

        related_items = object_data.get("relatedItems")
        return
        if not related_items:
            return

        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@setuid"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(uid=uid), cls=CustomJSONEncoder),
        )
        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting UID: {url}: {result.text}", response=result
            )

    @timeit
    def _set_deferred_uids(self, resource_path, mapping):
        """ added portlets """

        url = f"{self.config.plone.url}{resource_path}/@@set-deferred-uids"
        LOG.info(
            f'_set_deferred_uids(path: "{resource_path}":\n{pprint.pformat(mapping)}'
        )
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(mapping, cls=CustomJSONEncoder),
        )
        check_204_response(result)

    @timeit
    def _set_owner(self, resource_path, object_data):
        """ added portlets """

        owner = object_data['_owner']
        owner = (['plone_portal', 'acl_users'], owner)

        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-owner"
#        LOG.info(f'_set_owner(path: "{resource_path}":\n{pprint.pformat(owner)}')
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(owner=owner), cls=CustomJSONEncoder),
        )
        check_204_response(result)


    @timeit
    def _set_portlets(self, resource_path, object_data):
        """ added portlets """

        portlets = object_data.get("_portlets")
        if not portlets:
            return

        for column, column_portlets in portlets.items():

            for column_portlet in column_portlets:

                # <collective.portlet.links.portlet.Assignment at object ....>
                name = column_portlet["name"]
                name, dummy = name[1:].split(" ", 1)

                data = {
                    "portlet_manager": column,
                    "portlet_data": column_portlet["data"],
                    "class": name,
                }

                url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@add-portlet"
                LOG.info(
                    f'_set_portlet(path: "{resource_path}":\n{pprint.pformat(data)}'
                )
                result = self.requests_session.post(
                    url,
                    auth=self._auth,
                    headers=self._json_headers,
                    data=json.dumps(data, cls=CustomJSONEncoder),
                )
                #                check_204_response(result)
                if result.status_code != 204:
                    print(f"ERROR: {result.text}")

    #                    raise MigrationError(
    #                        f"Error setting UID: {url}: {result.text}", response=result
    #                    )

    @timeit
    def _set_uid(self, resource_path, object_data):
        """ Set UID to original UID
        """

        uid = object_data["_uid"]
        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@setuid"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(uid=uid), cls=CustomJSONEncoder),
        )
        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting UID: {url}: {result.text}", response=result
            )

    @timeit
    def _set_portlet_blacklist(self, resource_path, object_data):
        """ Set UID to original UID
        """

        blacklist = object_data.get('_portlets_blacklist', {})
        if not blacklist:
            return

        for portlet_manager in ('plone.leftcolumn', 'plone.rightcolumn'):
            bl = blacklist.get(portlet_manager)
            if not bl:
                continue

            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@blacklist-portlets"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(dict(portlet_manager=portlet_manager, blacklist=1), cls=CustomJSONEncoder),
            )
            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting setting portlet blacklist: {url}: {result.text}", response=result
                )

    @timeit
    def _set_default_page(self, resource_path, object_data):
        """ Set default page
        """

        default_page = object_data.get('_defaultpage')
        if not default_page:
            return

        url = f"{self.config.plone.url}/{resource_path}/@@set-default-page"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(default_page=default_page), cls=CustomJSONEncoder),
        )
        check_204_response(result)

        # ignore 404
        if result.status_code == 404: # not found
            LOG.error(f"Error setting default page (404): {url}: {result.text}")
            return

        if result.status_code != 204:
            raise MigrationError(
                f"Error setting default page: {url}: {result.text}", response=result
            )

    @timeit
    def _set_layout(self, resource_path, object_data):
        """ Set default page
        """

        layout = object_data.get('_layout')
        if not layout:
            return

        # see PCM-1818
        if layout in ('fg_base_view_p3', 'blog_view', 'facetednavigation_view', 'list.html', 'sliderview', 'phddefense_view', 'manualgroup_view'):
            return

        if layout == 'atct_album_view':
            layout = 'album_view'

        if layout == 'atct_topic_view':
            layout = 'listing_view'

        if layout == 'folder_full_view':
            layout = 'full_view'

        # PCM-1818
        if object_data["_type"] in ["Topic"]:
            if layout == 'folder_listing_standardview':
                layout = 'listing_view'

        if layout == 'folder_summary_view':
            layout = 'summary_view'

        if layout in ('sortable_view', 'sortable_view_unbatched', 'folder_tabular_view'):
            layout = 'tabular_view'


        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-layout"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(layout=layout), cls=CustomJSONEncoder),
        )
        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting default page: {url}: {result.text}", response=result
            )

    @timeit
    def _set_position_in_parent(self, resource_path, object_data):
        """ Set position of object in parent """

        position = object_data["_gopip"]
        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-position-in-parent"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(dict(position=position), cls=CustomJSONEncoder),
        )
        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting setting position: {url}: {result.text}", response=result
            )

    @timeit
    def _set_allowed_and_addable_types(self, resource_path, object_data):
        """ Folder restrictions and addable types """

        constrain_types_mode = object_data.get("constrainTypesMode", -1)  # -1 = ACQUIRE
        addable_types = object_data.get("immediatelyAddableTypes", ())
        allowed_types = object_data.get("locallyAllowedTypes", ())

        if constrain_types_mode != -1 and (allowed_types or addable_types):
            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-allowed-and-addable-types"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(
                    dict(
                        allowed_types=allowed_types,
                        addable_types=addable_types,
                        constrain_types_mode=constrain_types_mode,
                    )
                ),
            )
            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting allowed/addable types: {url}: {result.text}",
                    response=result,
                )

    @timeit
    def _update_all_imagerefs(self):
        """ Update all `imageref` fields of `News Item` instances
            `imagerefs ` is a dict[uid] = uid_image
        """

        url = f"{self.config.plone.url}/{self.config.site.id}/@@update-all-imagerefs"
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(self._all_imagerefs),
        )

        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error updating imagesrefs: {url}: {result.text}", response=result
            )

    @timeit
    def _update_all_related_items(self):
        """ Update all related items
            `related_items` is a dict[uid] = [list of referenced uids]
        """

        url = (
            f"{self.config.plone.url}/{self.config.site.id}/@@update-all-related-items"
        )
        result = self.requests_session.post(
            url,
            auth=self._auth,
            headers=self._json_headers,
            data=json.dumps(self._all_related_items),
        )

        check_204_response(result)
        if result.status_code != 204:
            raise MigrationError(
                f"Error updating related items: {url}: {result.text}", response=result
            )

    @timeit
    def _set_created_modified(self, resource_path, object_data):
        """ Set created + modified timestamps """

        created = object_data.get("creation_date")
        modified = object_data.get("modification_date")
        if modified or created:
            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-created-modified"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(
                    dict(created=created, modified=modified), cls=CustomJSONEncoder
                ),
            )
            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting created+modified: {url}: {result.text}",
                    response=result,
                )

    @timeit
    def _set_permissions(self, resource_path, object_data):
        """ Set marker interfaces """

        permissions = object_data.get("_permissions", ())
        permissions = dict(
            [(k, v) for k, v in permissions.items() if k not in IGNORED_PERMISSIONS]
        )

        if permissions:
            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-permissions"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(dict(permissions=permissions), cls=CustomJSONEncoder),
            )

            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting marker interfaces: {url}: {result.text}",
                    response=result,
                )

    @timeit
    def _set_marker_interfaces(self, resource_path, object_data):
        """ Set marker interfaces """

        directly_provided = object_data.get("_directly_provided", ())
        directly_provided = [
            iface for iface in directly_provided if iface in SUPPORTED_MARKER_INTERFACES
        ]

        if directly_provided:
            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-marker-interfaces"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(
                    dict(interfaces=directly_provided), cls=CustomJSONEncoder
                ),
            )
            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting marker interfaces: {url}: {result.text}",
                    response=result,
                )

    @timeit
    def _set_local_roles(self, resource_path, object_data):
        """ Set local roles
            https://plonerestapi.readthedocs.io/en/latest/sharing.html
        """

        local_roles = object_data["_ac_local_roles"]
        if not local_roles:
            return

        block_local_roles = object_data.get("_ac_local_roles_block", False)

        entries = list()
        for username, roles in local_roles.items():
            entries.append(
                dict(
                    id=username,
                    type="user",
                    roles=dict([(role, True) for role in roles]),
                )
            )

        if entries:
            url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@sharing"
            result = self.requests_session.post(
                url,
                auth=self._auth,
                headers=self._json_headers,
                data=json.dumps(dict(entries=entries, inherit=not block_local_roles), cls=CustomJSONEncoder),
            )
            check_204_response(result)
            if result.status_code != 204:
                raise MigrationError(
                    f"Error setting local roles: {url}: {result.text}", response=result
                )


    def _set_unavailable(self, resource_path, object_data):
        """ collective.unavailable """

        directly_provided = object_data.get("_directly_provided", ())
        if 'collective.unavailable.interfaces.IUnavailable' not in directly_provided:
            return

        unavailable_annotations = object_data['_annotations']['collective.unavailable']
        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/set-unavailable"
        result = self.requests_session.post(
            url, auth=self._auth, headers=self._json_headers, data=json.dumps(unavailable_annotations, cls=CustomJSONEncoder),
        )
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting unavailable data: {url}: {result.text}",
                response=result,
            )

    def _set_review_state(self, resource_path, object_data):
        """ Set review state based on workflow history """

        review_state = object_data.get("review_state")
        if not review_state:
            return
        data = dict(review_state=review_state)
        url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@@set-review-state"
        result = self.requests_session.post(
            url, auth=self._auth, data=json.dumps(data), headers=self._json_headers
        )
        if result.status_code != 204:
            raise MigrationError(
                f"Error setting review state: {url}: {result.text}",
                response=result,
            )

    def _set_review_state_old(self, resource_path, object_data):
        """ Set review state based on workflow history """

        review_state = object_data["review_state"]
        # images are a published by default (see ugent_image_workflow)
        if object_data["_type"] == "Image":
            return

        # https://jira.collective.be/browse/PCM-1864
#        if object_data["_type"] != 'Folder' and review_state in ["private"]:  # nothing to do
#            return

        # map review state to transition name for moving an object from default
        # "visible" state into target state (visible is the default state of ugent* workflows
        state2action = {
            # default state -> nothing to do
            "private": "hide",
            "visible": "hide",
            "published": "publish",
            "internal": "makeinternal",
        }

        actions = None
        if f"{review_state}_{object_data['_type']}" in state2action:
            actions = state2action[f"{review_state}_{object_data['_type']}"]
        elif review_state in state2action:
            actions = state2action[review_state]
        else:
            LOG.info(f'Unsupported review_state "{review_state}"')
            return

        if actions:
            for action in actions.split("|"):
                url = f"{self.config.plone.url}/{self.config.site.id}/{resource_path}/@workflow/{action}"
                result = self.requests_session.post(
                    url, auth=self._auth, headers=self._json_headers
                )
                if result.status_code != 200:
                    raise MigrationError(
                        f"Error setting review state: {url}: {result.text}",
                        response=result,
                    )

    def migrate_folder(self, folder_name):
        LOG.info("*" * 80)
        LOG.info(f"migrating folder {folder_name}")
        LOG.info("*" * 80)

        # remove remote folder before migration
        if self.args.remove_remote_folders:
            self._delete_resource(folder_name)

        query = f"""
            FOR doc in  {self.collection_name}
                FILTER '{folder_name}' in doc._paths_all || '{folder_name}' == doc._path
                LIMIT 99999999999
                RETURN {{path: doc._path,
                        portal_type: doc._type,
                        id: doc._id,
                        title: doc.title,
                        _key: doc._key
                       }}
                """

        result = self._query_aql(query)
        result = sorted(result, key=lambda x: x["portal_type"])

        # group exportable items by portal_type
        result_by_portal_type = itertools.groupby(result, lambda x: x["portal_type"])

        # recreate folder structure
        result_by_portal_type = itertools.groupby(result, lambda x: x["portal_type"])
        all_folder_keys = list()  # remember folder keys for later

        # remember top-level folder for folder constraints
        folder_data = self._object_by_path(folder_name)
        all_folder_keys.append(folder_data["_key"])

        import pdb; pdb.set_trace()
        for portal_type, items in result_by_portal_type:

            if portal_type not in FOLDERISH_PT:
                continue
            LOG.info(f"Processing portal_type: {portal_type}")

            # sort folder paths by depth
            items = sorted(items, key=lambda x: x["path"].count("/"))
            import pdb; pdb.set_trace()
            num_items = len(items)
            for i, item in enumerate(items):

                LOG.info(f"{i+1}/{num_items} Folder {item['path']}")
                path_components = item["path"].split("/")
                path_components = [
                    pc for pc in path_components if pc and pc != self.config.site.id
                ]

                # construct all parent paths and check if they exist on the target site
                # and if necessary recreate the related content
                for i in range(len(path_components)):
                    parent_path = "/".join(path_components[:i])
                    if not parent_path:
                        continue

                    # check if parent_path exists on remote_portal
                    if parent_path in PARENT_EXISTS_CACHE:
                        parent_path_exists = True
                    else:
                        parent_path_exists = self.remote_exists(parent_path)
                        PARENT_EXISTS_CACHE[parent_path] = True

                    # parent path exists -> nothing to do
                    if parent_path_exists:
                        continue

                    # create new folderish object
                    parent_path_full = f"/{self.config.site.id}/{parent_path}"
                    parent_data = self._object_by_path(parent_path_full)
                    self._create_object(parent_path, parent_data["_key"])

                # now reconstruct item content after we are sure that the parent
                # structure exists

                item_path = "/".join(
                    item["path"].split("/")[2:]
                )  # omit empty "" and portal_id
                self._create_object(item_path, item["_key"])
                all_folder_keys.append(item["_key"])

        # now content
        # we need to re-groupby because groupby() returns an iterator
        result_by_portal_type = itertools.groupby(result, lambda x: x["portal_type"])
        for portal_type, items in result_by_portal_type:

            # folderish hierarchy is already created -> skip processing
            if portal_type in FOLDERISH_PT:
                continue

            if (
                self.config.migration.excluded_content_types
                and portal_type in self.config.migration.content_types
            ):
                LOG.info(
                    f'Skipping processing of "{portal_type}" due to `excluded_content_types` configuration'
                )
                continue

            if (
                self.config.migration.content_types
                and not portal_type in self.config.migration.content_types
            ):
                LOG.info(
                    f'Skipping processing of "{portal_type}" due to `content_types` configuration'
                )
                continue

            items = [x for x in items]
            num_items = len(items)
            for i, item in enumerate(items):

                if portal_type in PROCESSED_TYPES:
                    item_path = "/".join(
                        item["path"].split("/")[2:]
                    )  # omit empty "" and portal_id
                    LOG.info(f"{i+1}/{num_items} {portal_type} {item_path}")
                    try:
                        self._create_object(item_path, item["_key"])
                    except Exception as e:
                        LOG.info(f'MigrationError: {item["path"]}: {e}', exc_info=True)

        # final fixup for folders
        for key in all_folder_keys:
            object_data = self._object_by_key(key)
            # add folder restrictions to folderish objects
            self._set_allowed_and_addable_types(
                object_data["_relative_path"], object_data
            )

        # migrate all relatedItems after all content has been created
        self._update_all_related_items()
        self._update_all_imagerefs()

    def migrate_portlets(self):
        for _key, resource_path in self._content_with_portlets.items():
            object_data = self._object_by_key(_key)
            self._set_portlets(resource_path, object_data)

    def migrate_deferred_uids(self):
        for _key, mapping in self._deferred_uids.items():
            object_data = self._object_by_key(_key)
            if mapping:
                self._set_deferred_uids(object_data['_path'], mapping)

    def migrate_deferred_default_pages(self):
        for _key, default_page in self._deferred_default_pages.items():
            object_data = self._object_by_key(_key)
            self._set_default_page(object_data['_path'], object_data)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", help="configuration file for migration (YAML format)", required=True
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        dest="ignore_safety_questions",
        default=False,
        help="Ignore safety questions",
    )
    parser.add_argument(
        "-k",
        "--keep-site",
        action="store_true",
        dest="keep_site",
        default=False,
        help="Incremental import - don't wipe out target plone site",
    )
    parser.add_argument(
        "-r",
        "--remove-root-folders",
        action="store_true",
        dest="remove_remote_folders",
        default=False,
        help="Incremental import - remote root folders before import",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose mode (timing)"
    )
    parser.add_argument(
        "-l", "--log-requests", action="store_true", help="Requests low-level logging"
    )
    args = parser.parse_args()

    if args.verbose:
        global VERBOSE
        VERBOSE = True

    # enable verbose logging for `requests` module
    if args.log_requests:
        import requests
        import logging
        import http.client as http_client

        http_client.HTTPConnection.debuglevel = 1

        # You must initialize logging, otherwise you'll not see debug output.
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    # read YAML configuration
    yaml_fn = os.path.abspath(args.config)
    LOG.info(f"Reading {yaml_fn}")
    if not os.path.exists(yaml_fn):
        raise IOError(f"Migration configuration {yaml_fn} not found")

    with open(yaml_fn) as fp:
        config = attrdict.AttrDict(yaml.load(fp, Loader=yaml.FullLoader))
    pprint.pprint(config)

    # prepare Migrator instance with YAML configuration and commandline options
    migrator = Migrator(config, args)

    # check languages (triggers an exception if language configuration is missing)
    default_language = migrator.default_language

    # folder pre-check
    for name in config.migration.folders:
        if name.endswith("/"):
            raise ValueError(
                f"migration folder path {name} must not end with a trailing slash - please remove it"
            )
        data = migrator._object_by_path(name)
        if not data:
            raise ValueError(f"No source information found for {name}")
        LOG.info(f"Precheck OK for {name}")

    if not args.keep_site:
        if args.ignore_safety_questions:
            migrator.create_plone_site()
        else:
            if query_yes_no("Clear and recreate remote Plone site?"):
                migrator.create_plone_site()
            else:
                LOG.warn("Aborting....")
                sys.exit(1)
    else:
        LOG.info("Skipping site creation (incremental mode)")


    LOG.info(f"Migration prepare")
    migrator.prepare()

    LOG.info(f"Reading vocabularies")
    migrator.read_vocabularies(INTROSPECT_VOCABULARIES)

    LOG.info(f"Content type filter: {config.migration.content_types}")

    # run real migration
    for name in config.migration.folders:
        migrator.migrate_folder(name)

    migrator.migrate_portlets()
    migrator.migrate_deferred_uids()
    migrator.migrate_deferred_default_pages()

    migrator.fixup()


if __name__ == "__main__":
    main()
