# Migrator for ATTopic -> Plone 5 new-style collections

import pprint


PT_REPLACEMENTS = {
    "Page": "Document"
}


# customViewFields (requires portal_catalog metadata)
field_mapping = {
    'contact_name': 'contact_name',
    'getCategories': 'categories',
    'getContract': 'contract',
    'getDefenseDateAndTime': 'date_and_time',
    'getDepartment': 'department',
    'getDepartmentName': 'get_department_name_nl',
    'getDepartmentEnglishName': 'get_department_name_en',
    'getFaculty': 'faculty',
    'getFacultyEnglishName': 'get_faculty_name_en',
    'getFullName': 'get_full_name',
    'getGrade': 'grade',
    'getLastApplicationDate': 'last_application_date',
    'getOccupancyRate': 'occupancy_rate',
}


# criteria fields (requires portal_catalog indexes)
criteria_mapping = {
    'getCategories': 'categories',
    'getDefenseDateAndTime': 'date_and_time',
    'getDepartment': 'department',
    'getFaculty': 'faculty',
    'getLastApplicationDate': 'last_application_date',
    'getOccupancyRate': 'occupancy_rate',
    'getVacancyType': 'vacancy_type',
    'is_image_folder': 'is_image_folder',
}


class TopicMigrator:
    def __init__(self, migration_data, object_data, child_keys, migrator, log):
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
        self.log = log
        self.query = list()

    def add_query(self, sub_query):
        self.query.append(sub_query)

    def migrate(self):

        self.migration_data["text"] = self.object_data["text"]
        self.migration_data["item_count"] = self.object_data["itemCount"] or 9999999
        self.log.info
        customViewFields = self.object_data["customViewFields"]
        customViewFields = [field_mapping.get(v, v) for v in customViewFields]
        self.log.info(f'customViewFields: {self.object_data["customViewFields"]} -> {customViewFields}')
        self.migration_data["customViewFields"] = customViewFields

        for _key in self.child_keys:
            child_data = self.migrator._object_by_key(_key)

            migrator_method = getattr(self, "migrate_" + child_data["_type"], None)
            if not migrator_method:
                self.log.info(f"unable to migrate {child_data['_type']}")
                continue
            try:
                migrator_method(child_data)
            except Exception as e:
                self.log.error('Unable to migrate criterion ({})'.format(e))

        self.log.info(f'query: {self.query}')
        self.migration_data["query"] = self.query

    def migrate_ATListCriterion(self, object_data):

        items = object_data["value"]
        operator = object_data["operator"]
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        q = dict(
            i=field2,
            o="plone.app.querystring.operation.selection.{}".format(
                "any" if operator == "or" else "all"
            ),
            v=items,
        )
        self.add_query(q)

    def migrate_ATSortCriterion(self, object_data):
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        self.migration_data["sort_reversed"] = object_data["reversed"]
        self.migration_data["sort_on"] = field2

    def migrate_ATDateCriteria(self, object_data):
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        operation = object_data["operation"]
        assert operation in ("more", "less"), f'operation "{operation}" unknown'
        value = object_data["value"]

        # we handle only relative dates here
        if operation == "more":
            if value == 0:
                operation = "plone.app.querystring.operation.date.afterToday"
            else:
                operation = (
                    "plone.app.querystring.operation.date.lessThanRelativeDate"
                )
        elif operation == "less":
            if value == 0:
                operation = "plone.app.querystring.operation.date.beforeToday"
            else:
                operation = "plone.app.querystring.operation.date.largerThanRelativeDate"

        q = dict(i=field2, o=operation, v=str(value))
        self.add_query(q)

    def migrate_ATPortalTypeCriterion(self, object_data):

        from collective.plone5migration.migration.migration_import import IGNORED_TYPES

        pt = object_data["value"]
        if isinstance(pt, (list, tuple)):
            pt = [PT_REPLACEMENTS.get(p, p) for p in pt]
        else:
            pt = PT_REPLACEMENTS.get(pt, pt)
        if pt in IGNORED_TYPES:
            return
        q = dict(
            i="portal_type",
            o="plone.app.querystring.operation.selection.any",
            v=pt
        )
        self.add_query(q)

    def migrate_ATPathCriterion(self, object_data):
        value = object_data.get('value')
        if not value:
            return
        for uid in value:
            value = uid
            if object_data["recurse"]:
                value += "::-1"
            else:
                value += "::1"

            q = dict(
                i="path",
                o="plone.app.querystring.operation.string.absolutePath",
                v=value
            )
            self.add_query(q)

    def migrate_ATRelativePathCriterion(self, object_data):
        field = object_data["field"]
        recursive = object_data["recurse"]
        value = object_data["relativePath"]

        if value in (".", ".."):
            operation = "plone.app.querystring.operation.string.relativePath"
            value = value + "::1"
        elif value.startswith("/"):
            operation = "plone.app.querystring.operation.string.absolutePath"
        elif value.startswith("../"):
            operation = "plone.app.querystring.operation.string.relativePath"
        else:
            operation = "plone.app.querystring.operation.string.relativePath"

        q = dict(i=field, o=operation, v=value)
        self.add_query(q)

    def migrate_ATSelectionCriterion(self, object_data):
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["value"]
        operator = object_data["operator"]

        if field == "Subject":
            if operator == "and":
                operation = "plone.app.querystring.operation.selection.all"
            else:
                operation = "plone.app.querystring.operation.selection.any"
        else:
            operation = "plone.app.querystring.operation.selection.any"

        q = dict(i=field2, o=operation, v=value)
        self.add_query(q)

    def migrate_ATSimpleIntCriterion(self, object_data):
        # ATT: untested, all getOccupancyRates criteria are empty
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["value"]
        direction = object_data.get("range")
        if not value:
            return
        if not direction:
            code = "is"
        elif direction == "min":
            code = "largerThan"
        elif direction == "max":
            code = "lessThan"
        elif direction == "min:max":
            self.log.warn("min:max is not suported for integers")
            return
        operation = "plone.app.querystring.operation.int.{}".format(code)

        q = dict(i=field2, o=operation, v=value)
        self.add_query(q)

    def migrate_ATSimpleStringCriterion(self, object_data):
        # ATT: untested, all getOccupancyRates criteria are empty
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["value"]
        operation = "plone.app.querystring.operation.selection.any"

        q = dict(i=field2, o=operation, v=[value])
        self.add_query(q)

    def migrate_ATSimpleIntCriterion(self, object_data):
        # ATT: untested, all getOccupancyRates criteria are empty
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["value"]
        direction = object_data.get("range")
        if not value:
            return
        if not direction:
            code = "is"
        elif direction == "min":
            code = "largerThan"
        elif direction == "max":
            code = "lessThan"
        elif direction == "min:max":
            self.log.warn("min:max is not suported for integers")
            return
        operation = "plone.app.querystring.operation.int.{}".format(code)

        q = dict(i=field2, o=operation, v=value)
        self.add_query(q)

    def migrate_ATSimpleStringCriterion(self, object_data):
        # ATT: untested, all getOccupancyRates criteria are empty
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["value"]
        operation = "plone.app.querystring.operation.selection.any"

        q = dict(i=field2, o=operation, v=[value])
        self.add_query(q)

    def migrate_ATBooleanCriterion(self, object_data):
        field = object_data["field"]
        field2 = criteria_mapping.get(field, field)
        value = object_data["bool"]
        if value in [1, True, "1", "True"]:
            operation = "plone.app.querystring.operation.boolean.isTrue"
        elif value in [0, "", False, "0", "False", None, (), {}]:
            operation = "plone.app.querystring.operation.boolean.isFalse"

        q = dict(i=field2, o=operation, v=[value])
        self.add_query(q)

    def migrate_ATDateRangeCriterion(self, object_data):
        """ Not implemented """
        self.log.warn("Not implemented: ATDateRangeCriterion")

    def migrate_ATCurrentAuthorCriterion(self, object_data):
        """ Not implemented """
        self.log.warn("Not implemented: ATCurrentAuthorCriterion")

    def migrate_ATReferenceCriterion(self, object_data):
        self.log.warn("Not implemented: ATReferenceCriterion")
        """ Not implemented """
