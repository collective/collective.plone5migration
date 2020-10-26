# -*- coding: utf8 -*_
from BTrees.OOBTree import OOBTree
from DateTime.DateTime import DateTime
from OFS.interfaces import IOrderedContainer
from plone.app.textfield.value import RichTextValue
from plone.app.theming.browser.controlpanel import ThemingControlpanel
from plone.protect.interfaces import IDisableCSRFProtection
from Products.CMFPlone.factory import addPloneSite
from Products.CMFPlone.interfaces import ISelectableConstrainTypes
from Products.Five.browser import BrowserView
from Products.Five.utilities.marker import mark
from Products.CMFCore.WorkflowTool import IWorkflowStatus
from plone.portlets.interfaces import IPortletManager
from plone.portlets.interfaces import IPortletAssignmentMapping
from plone.portlets.interfaces import ILocalPortletAssignmentManager
from plone.portlets.constants import CONTEXT_CATEGORY
from plone.contentrules.engine.interfaces import IRuleStorage
from zope.component import getMultiAdapter
from zope.container.interfaces import INameChooser
from DateTime.DateTime import DateTime

from z3c.relationfield import RelationValue
from zope.component import createObject
from zope.component import getUtility
from zope.component import getMultiAdapter
from zope.interface import alsoProvides
from zope.intid.interfaces import IIntIds

import dateutil
import dateutil.parser
import importlib
import json
import lxml.html
import os
import importlib
import inspect
import pkg_resources
import plone.api
import transaction
import zExceptions


class MyThemingControlpanel(ThemingControlpanel):
    def authorize(self):
        return True


class API(BrowserView):
    def __init__(self, context, request):
        self.request = request
        self.context = context
        super().__init__(context, request)
        alsoProvides(request, IDisableCSRFProtection)

    def recreate_plone_site(self):
        """ Recreate a Plone site """

        # remove /temp_folder first, if existing
        if 'temp_folder' in self.context.objectIds():
            self.context.manage_delObjects(['temp_folder'])

        data = json.loads(self.request.BODY)
        site_id = str(data["site_id"])
        extension_ids = data["extension_ids"]
        theme = data.get("theme", "barceloneta")

        root = self.context.restrictedTraverse("/")
        if site_id in root.objectIds():
            print('Deleting Plone site "{0}"'.format(site_id))
            root.manage_delObjects([site_id])

        print('Creating Plone site "{0}" with {1}'.format(site_id, extension_ids))
        #        addPloneSite(root, site_id, extension_ids=extension_ids)
        addPloneSite(root, site_id)
        print('Created Plone site "{0}" with {1}'.format(site_id, extension_ids))

        site = root[site_id]
        qi = site.portal_quickinstaller
        for extension_id in (extension_ids or ()):
            try:
                product, profile = extension_id.split(":")
            except ValueError:
                product, profile = extension_id, "default"
            print("Installing {}".format(extension_id))
            qi.installProducts([product])

        self.request.form["form.button.Enable"] = "DONE"
        self.request.form["themeName"] = theme
        view = MyThemingControlpanel(root[site_id], self.request)
        view.update()

        # disable content rules (PCM-1773)
        storage = getUtility(IRuleStorage)
        storage.active = False

        # set portal.language to NL
        # See https://community.plone.org/t/content-types-control-panel-zope-schema-bootstrapinterfaces-constraintnotsatisfied-en-us-language/12761
        site.language = 'nl'

        # update portal_diff settings
        # https://jira.collective.be/browse/PCM-1591
        portal_diff = site.portal_diff
        for pt in ['SubsiteFolder']:
            portal_diff._pt_diffs[pt] =  {'any': 'Compound Diff for Dexterity types'}

        self.request.response.setStatus(201)
        return "DONE"

    def remote_exists(self, path):
        """ Check if `path` exists based on our own traversal.
            The purpose of this method is to provide a traversal
            lookup that is not dependent on Acquisition but on
            real traversal.
            E.g. a request to `/plone/papers/conference/papers` would
            resolve to the first `papers` folder if the second
            `papers` folder does not exist.
        """
        #        current = self.context.restrictedTraverse("/")
        current = plone.api.portal.get()
        for c in path.split("/"):
            if not c:
                continue
            if c in current.objectIds():
                current = current[c]
            else:
                raise zExceptions.NotFound(path)
        self.request.response.setStatus(200)

    def setuid(self):
        """ Set given `uid` on current context object """
        data = json.loads(self.request.BODY)
        uid = data["uid"]
        setattr(self.context, "_plone.uuid", uid)
        self.context.reindexObject(idxs=["UID"])
        self.request.response.setStatus(204)

    def set_owner(self):
        """ Set owner tuple """
        data = json.loads(self.request.BODY)
        owner = data["owner"]
        setattr(self.context, "_owner", owner)
        self.context.reindexObjectSecurity()
        self.request.response.setStatus(204)

    def set_review_state(self):
        """ Directly set review_state. See
            https://community.plone.org/t/setting-review-state-quick-programmatically/12991
        """

        data = json.loads(self.request.BODY)
        review_state = data["review_state"]

        wf_tool = plone.api.portal.get_tool("portal_workflow")
        workflows = wf_tool.getWorkflowsFor(self.context.portal_type)
        if not workflows:
            self.request.response.setStatus(204)
            return

        wf = workflows[0]
        wfs = getMultiAdapter((self.context, wf), IWorkflowStatus)

        new_status = dict(
                action=None,
                actor=None,
                comments='Set by migration',
                review_state=review_state,
                time=DateTime())
        wfs.set(new_status)

        self.context.reindexObject(idxs=["review_state"])
        self.request.response.setStatus(204)

    def set_deferred_uids(self):
        """ Set given `uid` on current context object """
        data = json.loads(self.request.BODY)
        catalog = plone.api.portal.get_tool("portal_catalog")
        intids = getUtility(IIntIds)

        for field, uid_or_uids in data.items():
            if not uid_or_uids:
                continue

            # Single RelationValue
            if not isinstance(uid_or_uids, (list, tuple)):
                brains = catalog(UID=uid_or_uids)
                if brains:
                    rv = RelationValue(intids.getId(brains[0].getObject()))
                    setattr(self.context, field, rv)
            else:
                # RelationList
                result = []
                for uid in uid_or_uids:
                    brains = catalog(UID=uid)
                    if brains:
                        rv = RelationValue(intids.getId(brains[0].getObject()))
                        result.append(rv)
                setattr(self.context, field, result)

        self.request.response.setStatus(204)

    def set_created_modified(self):
        """ Set given `uid` on current context object """
        data = json.loads(self.request.BODY)
        created = data["created"]
        modified = data["modified"]
        if created:
            self.context.creation_date = DateTime(created)
        if modified:
            self.context.modification_date = DateTime(modified)
        self.context.reindexObject(idxs=["created", "modified"])
        self.request.response.setStatus(204)

    def convert_to_uids(self):
        """ Convert all links inside a RichText field from path to UID """
        from plone.protect.interfaces import IDisableCSRFProtection
        from zope.interface import alsoProvides

        alsoProvides(self.request, IDisableCSRFProtection)

        catalog = plone.api.portal.get_tool("portal_catalog")

        for brain in catalog():
            obj = brain.getObject()

            try:
                html = obj.text.raw
            except AttributeError:
                continue

            root = lxml.html.fromstring(html)
            for img in root.xpath("//img"):
                src = img.attrib["src"]
                # fix spelling error in lsoptsupport
                if "resolveUid" in src:
                    src = src.replace("resolveUid", "resolveuid")
                    img.attrib["src"] = src
                if src.startswith("resolveuid/"):
                    continue
                src_parts = src.split("/")
                scale = ""
                if src_parts[-1] in (
                    "image_preview",
                    "image_large",
                    "image_mini",
                    "image_thumb",
                    "image_tile",
                    "image_icon",
                    "image_listing",
                ):
                    src = "/".join(src_parts[:-1])
                    scale = src_parts[-1].replace("image_", "")
                target = self.context.restrictedTraverse(src, None)
                if target is not None:
                    img.attrib["src"] = "resolveuid/{}".format(target.UID())
                    class_ = img.attrib.get("class", "")
                    if scale:
                        img.attrib["class"] = "scale-{} ".format(scale) + class_

            html = lxml.html.tostring(root)
            obj.text = RichTextValue(html, "text/html", "text/html")

        self.request.response.setStatus(200)

    def set_navigationroot(self):
        """ Set INavigationRoot on current context object """
        from Products.Five.utilities.marker import mark
        from plone.app.layout.navigation.interfaces import INavigationRoot
        from plone.protect.interfaces import IDisableCSRFProtection
        from zope.interface import alsoProvides

        alsoProvides(self.request, IDisableCSRFProtection)
        mark(self.context, INavigationRoot)

        self.request.response.setStatus(200)

    def fix_languages(self):

        from zope.interface import alsoProvides
        from plone.protect.interfaces import IDisableCSRFProtection

        alsoProvides(self.request, IDisableCSRFProtection)

        portal = plone.api.portal.get()
        catalog = plone.api.portal.get_tool("portal_catalog")

        for language in ("en", "de"):
            if language not in portal.objectIds():
                continue

            brains = catalog(path="/" + portal.getId() + "/" + language)
            for i, brain in enumerate(brains):
                obj = brain.getObject()
                obj.setLanguage(language)
                obj.reindexObject(idxs=["Language"])
                if i % 500 == 0:
                    transaction.savepoint()

        return "DONE"

    def set_positions_in_parent(self):
        """ Set positions of container object given by `id` and `position` """

        data = json.loads(self.request.BODY)
        num_data = len(data)
        for i, item in enumerate(data):
            path = item['path']
            position = item ['position']
            print(i, num_data, path, position)
            obj = self.context.restrictedTraverse(path, None)
            if obj is None:
                continue

            try:
                ordered = IOrderedContainer(obj.aq_parent)
            except (AttributeError, TypeError) as e:
                print(e)
                continue

            try:
                ordered.moveObjectToPosition(obj.getId(), position)
            except ValueError as e:
                print(e)
                continue

            obj.reindexObject(idxs=['getObjPositionInParent'])
        self.request.response.setStatus(204)

    def set_position_in_parent(self):
        """ Set position of container object given by `id` and `position` """

        data = json.loads(self.request.BODY)
        position = int(data["position"])
        ordered = IOrderedContainer(self.context.aq_parent)
        ordered.moveObjectToPosition(self.context.getId(), position)
        self.request.response.setStatus(204)

    def set_allowed_and_addable_types(self):
        """ Folder restrictions and addable types """

        def modify_types(types):
            old2new = {"Topic": "Collection"}

            types_tool = plone.api.portal.get_tool("portal_types")
            available_types = types_tool.objectIds()

            # replace old types with new types
            types = [old2new.get(t, t) for t in types]
            # filter out non existing types
            types = [t for t in types if t in available_types]
            return types

        data = json.loads(self.request.BODY)
        addable_types = data.get("addable_types", ())
        addable_types = modify_types(addable_types)
        allowed_types = data.get("allowed_types", ())
        allowed_types = modify_types(allowed_types)
        constrain_types_mode = data.get("constrain_types_mode", -1)

        if constrain_types_mode == -1:  # AQUIRE
            return

        constrains = ISelectableConstrainTypes(self.context)
        if allowed_types:
            constrains.setLocallyAllowedTypes(allowed_types)
        if addable_types:
            constrains.setImmediatelyAddableTypes(addable_types)
        constrains.setConstrainTypesMode(constrain_types_mode)
        self.request.response.setStatus(204)

    def set_translation(self, original_path, translation_path, target_language):
        """ Set position of container object given by `id` and `position` """

        from plone.protect.interfaces import IDisableCSRFProtection
        from zope.interface import alsoProvides
        from plone.app.multilingual.interfaces import ITranslationManager

        alsoProvides(self.request, IDisableCSRFProtection)

        source_obj = self.context.restrictedTraverse(original_path, None)
        if source_obj is None:
            raise ValueError("No object found at {}".format(original_path))

        translated_obj = self.context.restrictedTraverse(translation_path, None)
        if translated_obj is None:
            raise ValueError("No object found at {}".format(translation_path))

        translated_obj.language = target_language
        translated_obj.reindexObject()

        manager = ITranslationManager(source_obj)
        manager.register_translation(translated_obj.language, translated_obj)

        self.request.response.setStatus(200)

    def set_translation_map(self):
        """ Set position of container object given by `id` and `position` """

        from plone.protect.interfaces import IDisableCSRFProtection
        from zope.interface import alsoProvides
        from plone.app.multilingual.interfaces import ITranslationManager

        alsoProvides(self.request, IDisableCSRFProtection)

        translation_map = json.loads(self.request.BODY)

        for translations in translation_map:
            if "en" not in translations or "de" not in translations:
                continue
            source_obj = self.context.restrictedTraverse(
                str(translations["de"]["path"]), None
            )
            translated_obj = self.context.restrictedTraverse(
                str(translations["en"]["path"]), None
            )

            if source_obj is not None and translated_obj is not None:
                print(translations)
                manager = ITranslationManager(source_obj)
                try:
                    manager.register_translation(
                        translated_obj.language, translated_obj
                    )
                except KeyError as e:
                    print(e)

        self.request.response.setStatus(200)

    def update_all_related_items(self):
        """ Update all related items"""

        intids = getUtility(IIntIds)

        catalog = plone.api.portal.get_tool("portal_catalog")
        all_related_items = json.loads(self.request.BODY)

        for uid, referenced_uids in all_related_items.items():
            brains = catalog(UID=uid)
            if not brains:
                print(f"UID {uid} not found")
                continue
            obj = brains[0].getObject()

            referenced_objs = []
            for uid in referenced_uids:
                brains = catalog(UID=uid)
                if brains:
                    referenced_objs.append(
                        RelationValue(intids.getId(brains[0].getObject()))
                    )

            obj.relatedItems = referenced_objs

        self.request.response.setStatus(204)

    def update_imagerefs(self):
        """ Update imageref fields (UGENT)"""

        intids = getUtility(IIntIds)

        catalog = plone.api.portal.get_tool("portal_catalog")
        all_imagerefs = json.loads(self.request.BODY)

        for uid, referenced_uid in all_imagerefs.items():
            brains = catalog(UID=uid)
            if not brains:
                print(f"UID {uid} not found")
                continue
            obj = brains[0].getObject()

            brains = catalog(UID=referenced_uid)
            if brains:
                obj.image_ref = RelationValue(intids.getId(brains[0].getObject()))

        self.request.response.setStatus(204)

    def set_permissions(self):
        """ Set marker interfaces on current object """

        data = json.loads(self.request.BODY)
        permissions = data["permissions"]
        for permission in permissions:
            acquire = permissions[permission]["acquire"]
            roles = permissions[permission]["roles"]
            try:
                self.context.manage_permission(permission, roles=roles, acquire=acquire)
            except Exception as e:
                print(f"Unable to set {permission}: {e}")
        self.context.reindexObjectSecurity()
        self.request.response.setStatus(204)

    def set_marker_interfaces(self):
        """ Set marker interfaces on current object """

        data = json.loads(self.request.BODY)
        interfaces = data["interfaces"]

        for iface in interfaces:
            module_name, iface_name = iface.rsplit(".", 1)
            try:
                module = importlib.import_module(module_name)
            except ImportError as e:
                print(f"Unable to import module {module_name}")
                raise e

            iface_object = getattr(module, iface_name, None)
            if iface_object is None:
                raise ValueError(
                    f"Unable to retrieve {iface_name} from module {module_name}"
                )

            mark(self.context, iface_object)

        self.request.response.setStatus(204)

    def get_indexes(self):
        """ Return a list of all portal_catalog indexes """

        catalog = plone.api.portal.get_tool("portal_catalog")
        result = [*catalog.indexes()]

        self.request.response.setHeader("content-type", "application/json")
        return json.dumps(result)

    def set_unavailable(self):
        """ """

        from collective.unavailable.interfaces import IUnavailableAnnotations

        unavailable_annotations = json.loads(self.request.BODY)

        annotations = IUnavailableAnnotations(self.context)
        annotations["layout"] = unavailable_annotations.get("layout", None)
        annotations["page"] = unavailable_annotations.get("page", None)
        expiration = unavailable_annotations.get("expiration")
        if expiration:
            annotations["expiration"] = DateTime(expiration)

        self.context.unavailable_url = unavailable_annotations.get("url")
        self.context.unavailable_text = unavailable_annotations.get("text")
        self.request.response.setStatus(204)

    def blacklist_portlets(self):
        """ Blacklist/block parent portlets """

        data = json.loads(self.request.BODY)
        portlet_manager = data["portlet_manager"]
        blacklist = data["blacklist"]

        manager = getUtility(IPortletManager, name=portlet_manager)
        blacklist = getMultiAdapter(
            (self.context, manager), ILocalPortletAssignmentManager
        )
        blacklist.setBlacklistStatus(CONTEXT_CATEGORY, blacklist)

        self.request.response.setStatus(204)

    def add_portlet(self):
        """ Add a portlet assignment.

            Example data:

            {
                "portlet_manager": "plone.rightcolumn",
                "title": "my title",
                "class": "collective.portlet.links.portlet.Assignment",
                "portlet_data": {
                    "links": [
                        {"url": "https://www.heise.de", "label": "Heise", "icon": "pdf"}
                    ]
                }
            }

        """

        intids = getUtility(IIntIds)

        data = json.loads(self.request.BODY)
        class_ = data["class"]
        portlet_manager = data["portlet_manager"]
        portlet_data = data["portlet_data"]

        # turn ugent portlet into standard plone collection portlet
        if class_ == "collective.portlet.collection.portlet.UGentCollectionAssignment":
#            class__ = "plone.portlet.collection.collection.Assignment"
            class__ = "collective.portlet.collection.portlet.Assignment"
            mod_name, class_name = class__.rsplit(".", 1)
        else:
            mod_name, class_name = class_.rsplit(".", 1)

        # create assignment from something like 'collective.portlet.infolinks.portlet.Assignment'
        try:
            module = importlib.import_module(mod_name)
        except ModuleNotFoundError:
            self.request.response.setStatus(
                501
            )  # indicate 501 error in order to avoid retry on client
            return f"Module {class_} not found"

        assignment_class = getattr(module, class_name)
        argspec = inspect.getargspec(assignment_class.__init__)

        # drop data that does not match the signature of the assignment (base class)
        if "UGentCollectionAssignment" in class_:
#            portlet_data["uid"] = portlet_data["target_collection_uid"]
            target_collection_path = portlet_data["target_collection"]
            if target_collection_path is None:
                return
            portlet_data['uid'] = portlet_data['target_collection_uid']
            del portlet_data["target_collection"]
            del portlet_data["__name__"]
            del portlet_data["target_collection_uid"]

        elif "slideshow" in class_:
            # images = [{path:..., uid:....}]
            images = []
            catalog = plone.api.portal.get_tool("portal_catalog")
            for d in portlet_data["images"]:
                brains = catalog(UID=d["uid"])
                if brains:
                    images.append(RelationValue(intids.getId(brains[0].getObject())))
            portlet_data["images"] = images

        # extract parameters from `portlet_data`
        params = dict()
        for k, v in portlet_data.items():
            if k in argspec.args:
                params[k] = v

        try:
            assignment = assignment_class(**params)
        except KeyError:
            # Assignment class may accept *args, **kw
            assignment = assignment_class(**portlet_data)

        column = getUtility(IPortletManager, portlet_manager)
        manager = getMultiAdapter((self.context, column), IPortletAssignmentMapping)
        chooser = INameChooser(manager)
        manager[chooser.chooseName(None, assignment)] = assignment

        self.request.response.setStatus(204)

    def set_layout(self):
        """ Set layout on container """

        data = json.loads(self.request.BODY)
        layout = data["layout"]
#        if layout not in [
#            "folder_listing_standardview",
#            "folder_listing_nosort",
#            "folder_listing_cronologic",
#        ]:
        self.context.setLayout(layout)
        self.request.response.setStatus(204)

    def set_default_page(self):
        """ Set default page on container """

        data = json.loads(self.request.BODY)
        default_page = data["default_page"]
        self.context.setDefaultPage(default_page)

        self.request.response.setStatus(204)

    def fixup(self):
        """ Last phase fixup steps """

        self.request.response.setStatus(200)
        return "DONE"

    def prepare(self):
        """ actions taken before the actual content migration """

        import pdb; pdb.set_trace()
        for id in ['news']:
            plone.api.content.delete(self.context[id])


        self.request.response.setStatus(200)
        return "DONE"
