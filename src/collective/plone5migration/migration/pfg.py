# -*- coding: utf-8 -*-

# PloneFormGen FormFolder -> Easyform migrator

import re
import copy
import collections
import lxml.etree
from html import escape as html_escape


def escape_xml(s):
    """ Escape XML element content """
    s = s.replace("&", "&amp;")
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    s = s.replace('"', '\\"')
    s = s.replace("'", "\\'")
    return s


def escape_attr(s):
    """ Escape XML attribute content """
    s = s.replace('"', "&quot;")
    return s


def fix_override(s):
    """ request/XXX to request/form.widget.XXX"""

    if 'request' not in s:
        return s

    parts = s.split('/')
    form_field = '/form.widgets.' + parts[-1]
    return '/'.join(parts[:-1]) + form_field



def migrate_vocabulary_override(v):
    if 'prefill_departments' in v:
        return 'collective.forms.easyform.departments'
    if 'getOptions' in v:
        # "python:[('','-')] + portal.restrictedTraverse('@@phddefense_helper').getOptions('collective.vocabularies.departments')",
        # "python:portal.restrictedTraverse('@@phddefense_helper').getOptions('collective.vocabularies.facultiesplus')",
        regex = re.compile("\'(collective.*)\'")
        mo = regex.search(v)
        if mo:
            s = mo.group(1)
            if ',' in s:
                return s.split(',')[0]
            else:
                return s
    return ''


def replace_expressions(xml):
    """ Replace old PloneFormGen expressions with new browser view implementations.
        The `memberXXXX` methods are defined in collective.forms.
    """
    xml = xml.replace(
        "folder.login2mail()", "object.restrictedTraverse('@@memberEmail')()"
    )
    xml = xml.replace(
        "folder.login2mail", "object.restrictedTraverse('@@memberEmail')()"
    )
    xml = xml.replace(
        "folder.login2email()", "object.restrictedTraverse('@@memberEmail')()"
    )
    xml = xml.replace(
        "folder.login2email", "object.restrictedTraverse('@@memberEmail')()"
    )
    xml = xml.replace(
        "folder.login2fullname()", "object.restrictedTraverse('@@memberFullname')()"
    )
    xml = xml.replace(
        "folder.login2ugentid()", "object.restrictedTraverse('@@memberUgentId')()"
    )
    xml = xml.replace(
        "folder.login2voornaam()", "object.restrictedTraverse('@@memberFirstname')()"
    )
    xml = xml.replace(
        "folder.login2voornaam", "object.restrictedTraverse('@@memberFirstname')()"
    )
    xml = xml.replace(
        "folder.login2name()", "object.restrictedTraverse('@@memberLastname')()"
    )
    xml = xml.replace(
        "folder.login2name", "object.restrictedTraverse('@@memberLastname')()"
    )
    xml = xml.replace(
        "folder.login2straat()", "object.restrictedTraverse('@@memberStreet')()"
    )
    xml = xml.replace(
        "folder.login2straat()", "object.restrictedTraverse('@@memberStreet')()"
    )
    xml = xml.replace(
        "folder.login2mobile()", "object.restrictedTraverse('@@memberMobile')()"
    )
    xml = xml.replace(
        "folder.login2mobile()", "object.restrictedTraverse('@@memberMobile')()"
    )
    xml = xml.replace(
        "folder.login2studentid()", "object.restrictedTraverse('@@memberStudentId')()"
    )
    xml = xml.replace(
        "folder.login2department()", "object.restrictedTraverse('@@memberDepartment')()"
    )
    xml = xml.replace(
        "folder.login2deptnaam()", "object.restrictedTraverse('@@memberDepartment')()"
    )
    xml = xml.replace(
        "folder.login2plaats()", "object.restrictedTraverse('@@memberLocation')()"
    )
    xml = xml.replace(
        "folder.login2adresdelen()",
        "object.restrictedTraverse('@@memberAddressParts')()",
    )
    xml = xml.replace("DateTime()", "object.restrictedTraverse('@@datetime_now')()")
    # only used for /plone_portal/nl/agenda/activiteit-toevoegen/wanneer/eind-datum
    # no replacement
    xml = xml.replace("python: folder.end_date_validator(value)", "")
    return xml


class PFGMigrator:
    def __init__(self, migration_data, object_data, child_keys, migrator):
        """
            `migration_data` - prefilled dict for plone.restapi call
            `object_data` - exported (JSON) data from collective.jsonify of FormFolder
            `child_keys` - list of _key value for all FormFolder childs
            `migrator` - main migrator
        """

        self.migration_data = migration_data
        self.object_data = object_data
        self.child_keys = child_keys
        self.migrator = migrator

    def _inspect_fieldsets(self):

        fieldsets = collections.OrderedDict()
        running = True
        current_fieldset = None
        ids_seen = list()

        # determine all fieldsets
        #
        # 'default' holds all fields not assigned to a particular FieldsetFolder
        # 'default-end' holds  all fields not assigned to a particular FieldseetFolder if other
        # FieldsetFolder were already assigned to 'fieldsets'
        fieldsets["default"] = dict(title="Default", description="Default", items=[])
        fieldsets["default-end"] = dict(
            title="Default (end)", description="Default (end)", items=[]
        )

        for _key in self.child_keys:
            child_data = self.migrator._object_by_key(_key)

            if child_data["_type"] == "FieldsetFolder":
                fieldsets[child_data["id"]] = dict(
                    title=child_data["title"],
                    description=child_data["description"],
                    items=[],
                )

        for _key in self.child_keys:
            child_data = self.migrator._object_by_key(_key)
            if child_data["_type"] in ("FieldsetFolder", "FormFolder"):
                continue

            parent_path_id = child_data["_paths_all"][-1].rsplit("/", 1)[-1]
            if parent_path_id in fieldsets:
                fieldsets[parent_path_id]["items"].append(child_data["id"])
            else:
                if len(fieldsets) == 2:  # no custom fieldsets
                    fieldsets["default"]["items"].append(child_data["id"])
                else:
                    fieldsets["default-end"]["items"].append(child_data["id"])

        # check default fieldsets for items (delete empty fieldset if empty)
        if not fieldsets["default"]["items"]:
            del fieldsets["default"]
        if not fieldsets["default-end"]["items"]:
            del fieldsets["default-end"]

        # always move 'default-end' fieldset to the end (OrderedDict!)
        if "default-end" in fieldsets:
            de = fieldsets["default-end"]
            del fieldsets["default-end"]
            fieldsets["default-end"] = de

        return fieldsets

    def migrate(self):
        fieldsets = self._inspect_fieldsets()

        _fields_xml = [
            """
            <model
                xmlns="http://namespaces.plone.org/supermodel/schema"
                xmlns:form="http://namespaces.plone.org/supermodel/form"
                xmlns:easyform="http://namespaces.plone.org/supermodel/easyform"
                xmlns:i18n="http://xml.zope.org/namespaces/i18n"
                i18n:domain="collective.easyform">
            """,
            "<schema>",
        ]

        actions = collections.OrderedDict()
        fields = collections.OrderedDict()

        # form defaults
        formPrologue = ""
        formEpilogoue = ""

        # Thanks page defaults
        tp_thanksPrologue = ""
        tp_thanksEpilogue = ""
        tp_thankstitle = ""
        tp_thanksdescription = ""
        tp_showAll = True
        tp_showFields = []
        tp_includeEmpties = False

        fields_added = list()
        action_fields_added = list()

        for _key in self.child_keys:
            json_data = self.migrator._object_by_key(_key)

            try:
                type_ = json_data["_type"]
                title = json_data.get("title", "")
                description = escape_xml(json_data.get("description", ""))
                id = json_data["id"]
                required = "True" if json_data.get("required") else "False"
                string_validator = json_data.get("fgStringValidator", "")
                hidden = "True" if json_data.get("hidden", False) else ""

                # field overrides
                TDefault = escape_attr(json_data.get("fgTDefault", ""))
                TValidator = escape_attr(json_data.get("fgTValidator", ""))
                TEnabled = escape_attr(json_data.get("fgTEnabled", ""))
                serverSide = json_data.get("serverSide", False)  # bool

            except Exception as e:
                print(f"ERROR in {json_data['_path']}", e)
                continue

            # skip already processed fields (JSON output may contain duplicate entries)
            if id in fields_added:
                continue

            validators = []
            if string_validator == "isEmail":
                validators.append("isEmail")

            validators = ",".join(validators)

            if type_ == "FormThanksPage":
                tp_thanksPrologue = json_data.get("thanksPrologue", "")
                tp_thanksEpilogue = json_data.get("thanksEpilogue", "")
                tp_thankstitle = json_data.get("title", "")
                tp_thanksdescription = json_data.get("description", "")
                tp_showAll = json_data.get("showAll", True)
                tp_showFields = json_data.get("showFields", [])
                tp_includeEmpties = json_data.get("includeEmpties", False)

            elif type_ == "FormMailerAdapter":
                body_pre = html_escape(json_data["body_pre"])
                body_post = html_escape(json_data["body_post"])
                # PCM-1691 don't migrate old-style template with references to AT
                # accessor methods and other culprit. Use EasyForm's default mailer
                # template instead
#                body_pt = html_escape(json_data["body_pt"])
                body_pt = ''
                cc_recipients = "\n".join(json_data["cc_recipients"])
                bcc_recipients = "\n".join(json_data["bcc_recipients"])
                additional_headers = "\n".join(
                    [
                        "<element>{}</element>".format(header)
                        for header in json_data.get("additional_headers", ())
                    ]
                )
                show_fields = "\n".join(
                    [
                        "<element>{}</element>".format(sf)
                        for sf in json_data.get("show_fields", ())
                    ]
                )
                j = json_data
                # https://jira.collective.be/browse/PCM-1901
                j = dict([(k, v if v not in ['#NONE#'] else '') for k,v in j.items()])
                actions[
                    id
                ] = f"""
                    <field name="{j['id']}" type="collective.easyform.actions.Mailer" easyform:execCondition="{j['execCondition']}">
                        <additional_headers>{additional_headers}</additional_headers>
                        <bccOverride>{fix_override(j['bccOverride'])}</bccOverride>
                        <bcc_recipients>{bcc_recipients}</bcc_recipients>
                        <body_footer>{j['body_footer']}</body_footer>
                        <!--
                        <body_post>{body_post}</body_post>
                        <body_pre>{body_pre}</body_pre>
                        <body_pt>{body_pt}</body_pt>
                        -->
                        <ccOverride>{fix_override(j['ccOverride'])}</ccOverride>
                        <cc_recipients>{cc_recipients}</cc_recipients>
                        <description>{j['description']}</description>
                        <recipientOverride>{fix_override(j['recipientOverride'])}</recipientOverride>
                        <recipient_email>{fix_override(j['recipient_email'])}</recipient_email>
                        <recipient_name>{fix_override(j['recipient_name'])}</recipient_name>
                        <replyto_field>{fix_override(j['replyto_field'])}</replyto_field>
                        <sendCSV>False</sendCSV>
                        <sendXML>False</sendXML>
                        <senderOverride>{fix_override(j['senderOverride'])}</senderOverride>
                        <showFields>{show_fields}</showFields>
                        <subjectOverride>{fix_override(j['subjectOverride'])}</subjectOverride>
                        <subject_field>{fix_override(j['subject_field'])}</subject_field>
                        <title>{j['title']}</title>
                        <to_field>{j['to_field']}</to_field>
                    </field>
                    """

            elif type_ == "FormSaveDataAdapter":
                actions[
                    id
                ] = """
                    <field name="Save adapter" type="collective.easyform.actions.SaveData">
                          <description/>
                         <title>Save adapter</title>
                    </field>
                    """

            elif type_ == "FormCustomScriptAdapter":
                script_body = json_data.get("ScriptBody", "")
                exec_condition = json_data.get("execCondition", "")
                proxy_role = json_data.get("ProxyRole", "")
                actions[
                    id
                ] = f"""
                    <field name="{id}" type="collective.easyform.actions.CustomScript" easyform:execCondition="{exec_condition}">
                          <description>{description}</description>
                          <title>{title}</title>
                          <ProxyRole>{proxy_role}</ProxyRole>
                          <ScriptBody>{script_body}</ScriptBody>
                    </field>
                    """

            elif type_ in ("FormTextField", "FormLinesField"):
                max_length = json_data.get("fgmaxlength", "")
                cols = json_data.get("fgCols", "60")
                rows = json_data.get("fgRows", "5")
                if max_length in ("0", "", None):
                    max_length = ""
                    widget = "z3c.form.browser.textarea.TextAreaFieldWidget"
                else:
                    widget = "collective.minmaxtextarea.browser.widget.MinMaxTextAreaFieldWidget"
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Text" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <max_length>{max_length}</max_length>
                              <required>{required}</required>
                              <form:widget type="{widget}">
                                  <rows>{rows}</rows>
                                  <cols>{cols}</cols>
                              </form:widget>
                            </field>
                    """

            elif type_ == "FormStringField":
                max_length = json_data.get("fgmaxlength", "")
                default = json_data.get('fgDefault', '')
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.TextLine" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <max_length>{max_length}</max_length>
                              <required>{required}</required>
                              <default>{default}</default>
                            </field>
                    """

            elif type_ == "FormFixedPointField":
                default = json_data.get('fgDefault', '')
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Float" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                              <default>{default}</default>
                            </field>
                    """

            elif type_ == "FormIntegerField":
                default = json_data.get('fgDefault', '')
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Int" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                              <default>{default}</default>
                            </field>
                    """

            elif type_ == "FormDateField":
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Datetime" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                            </field>
                    """

            elif type_ == "FormFileField":
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="plone.namedfile.field.NamedBlobFile" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                            </field>
                    """

            elif type_ == "FormBooleanField":
                default = json_data.get('fgDefault', 'False')
                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Bool" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                              <default>{default}</default>
                            </field>
                    """

            elif type_ in ("FormSelectionField"):
                old_values = json_data["fgVocabulary"]
                old_values = [escape_xml(v) for v in old_values]


                vocabulary = migrate_vocabulary_override(json_data.get('fgTVocabulary'))
                if vocabulary:
                    vocabulary = f"<vocabulary>{vocabulary}</vocabulary>"


                values = list()
                for s in old_values:
                    if "|" in s:
                        s1, s2 = s.split("|", 1)
                        values.append(
                            f"                  <element key='{s1}'>{s2}</element>"
                        )
                    else:
                        values.append(f"                  <element>{s}</element>")
                if values:
                    values = "<values>\n" + "\n".join(values) + "</values>"
                else:
                    values = ''

                widget = '<form:widget type="z3c.form.browser.radio.RadioFieldWidget"/>' if json_data.get('fgFormat') == 'radio' else''

                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.Choice" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                              {values}
                              {vocabulary}
                              {widget}
                            </field>
                    """

            elif type_ in ("FormMultiSelectionField"):
                old_values = json_data["fgVocabulary"]
                old_values = [escape_xml(v) for v in old_values]
                values = list()
                for s in old_values:
                    if "|" in s:
                        s1, s2 = s.split("|", 1)
                        values.append(
                            f"                  <element key='{s1}'>{s2}</element>"
                        )
                    else:
                        values.append(f"                  <element>{s}</element>")
                values = "\n".join(values)

                fields[
                    id
                ] = f"""
                            <field name="{id}" type="zope.schema.List" easyform:validators="{validators}" easyform:THidden="{hidden}" easyform:TDefault="{TDefault}" easyform:TValidator="{TValidator}" easyform:TEnabled="{TEnabled}" easyform:serverSide="{serverSide}">
                              <description>{description}</description>
                              <title>{title}</title>
                              <required>{required}</required>
                              <value_type type="zope.schema.Choice">
                                <values>{values}</values>
                              </value_type>
                              <form:widget type="z3c.form.browser.checkbox.CheckBoxFieldWidget"/>
                            </field>
                    """

        # build actions.xml
        actions_xml = "\n".join(
            [
                """
            <model
                xmlns="http://namespaces.plone.org/supermodel/schema"
                xmlns:form="http://namespaces.plone.org/supermodel/form"
                xmlns:easyform="http://namespaces.plone.org/supermodel/easyform"
                xmlns:i18n="http://xml.zope.org/namespaces/i18n"
                i18n:domain="collective.easyform">
            """,
                "<schema>",
                "\n".join(actions.values()),
                "</schema>",
                "</model>",
            ]
        )

        open("actions.xml", "w").write(actions_xml)
        try:
            actions_root = lxml.etree.fromstring(actions_xml)
        except Exception as e:
            print(e)
            import pdb

            pdb.set_trace()

        # build model.xml
        fieldsets_xml = list()
        for fs_id, fs_data in fieldsets.items():

            fs_title = fs_data["title"]
            fs_description = fs_data["description"]

            fieldsets_xml.append(
                f'<fieldset name="{fs_id}" label="{fs_title}" description="{fs_description}">'
            )

            for field_id in fs_data["items"]:
                if field_id in fields:
                    fieldsets_xml.append(fields[field_id])

            fieldsets_xml.append("</fieldset>")

        fields_xml = "\n".join(
            [
                """
            <model
                xmlns="http://namespaces.plone.org/supermodel/schema"
                xmlns:form="http://namespaces.plone.org/supermodel/form"
                xmlns:easyform="http://namespaces.plone.org/supermodel/easyform"
                xmlns:i18n="http://xml.zope.org/namespaces/i18n"
                i18n:domain="collective.easyform">
            """,
                "<schema>",
                "\n".join(fieldsets_xml),
                "</schema>",
                "</model>",
            ]
        )

        open("fields.xml", "w").write(fields_xml)
        try:
            fields_root = lxml.etree.fromstring(fields_xml)
        except Exception as e:
            print(e)
            import pdb

            pdb.set_trace()

        fields_xml = replace_expressions(fields_xml)
        root = lxml.etree.fromstring(fields_xml)
        fields_xml = lxml.etree.tostring(root, pretty_print=True, encoding=str)

        root = lxml.etree.fromstring(actions_xml)
        actions_xml = lxml.etree.tostring(root, pretty_print=True, encoding=str)

        # populate EasyForm instance
        self.migration_data["fields_model"] = fields_xml
        self.migration_data["actions_model"] = actions_xml
        self.migration_data["exclude_from_nav"] = True
        self.migration_data["form_tabbing"] = False

        # thanks page
        self.migration_data["thanksPrologue"] = tp_thanksPrologue
        self.migration_data["thanksEpilogue"] = tp_thanksEpilogue
        if tp_thankstitle:
            self.migration_data["thankstitle"] = tp_thankstitle
        self.migration_data["thanksdescription"] = tp_thanksdescription
        self.migration_data["showAll"] = tp_showAll
        self.migration_data["showFields"] = tp_showFields
        self.migration_data["includeEmpties"] = tp_includeEmpties
