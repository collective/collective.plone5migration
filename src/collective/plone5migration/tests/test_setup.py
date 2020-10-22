# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from collective.plone5migration.testing import UGENT_PLONE5MIGRATION_INTEGRATION_TESTING  # noqa

import unittest


class TestSetup(unittest.TestCase):
    """Test that collective.plone5migration is properly installed."""

    layer = UGENT_PLONE5MIGRATION_INTEGRATION_TESTING

    def setUp(self):
        """Custom shared utility setup for tests."""
        self.portal = self.layer['portal']
        self.installer = api.portal.get_tool('portal_quickinstaller')

    def test_product_installed(self):
        """Test if collective.plone5migration is installed."""
        self.assertTrue(self.installer.isProductInstalled(
            'collective.plone5migration'))

    def test_browserlayer(self):
        """Test that IUgentPlone5migrationLayer is registered."""
        from collective.plone5migration.interfaces import (
            IUgentPlone5migrationLayer)
        from plone.browserlayer import utils
        self.assertIn(
            IUgentPlone5migrationLayer,
            utils.registered_layers())


class TestUninstall(unittest.TestCase):

    layer = UGENT_PLONE5MIGRATION_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer['portal']
        self.installer = api.portal.get_tool('portal_quickinstaller')
        roles_before = api.user.get_roles(TEST_USER_ID)
        setRoles(self.portal, TEST_USER_ID, ['Manager'])
        self.installer.uninstallProducts(['collective.plone5migration'])
        setRoles(self.portal, TEST_USER_ID, roles_before)

    def test_product_uninstalled(self):
        """Test if collective.plone5migration is cleanly uninstalled."""
        self.assertFalse(self.installer.isProductInstalled(
            'collective.plone5migration'))

    def test_browserlayer_removed(self):
        """Test that IUgentPlone5migrationLayer is removed."""
        from collective.plone5migration.interfaces import \
            IUgentPlone5migrationLayer
        from plone.browserlayer import utils
        self.assertNotIn(
            IUgentPlone5migrationLayer,
            utils.registered_layers())
