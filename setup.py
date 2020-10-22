# -*- coding: utf-8 -*-
"""Installer for the collective.plone5migration package."""

from setuptools import find_packages
from setuptools import setup


long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CONTRIBUTORS.rst').read(),
    open('CHANGES.rst').read(),
])


setup(
    name='collective.plone5migration',
    version='5.3.dev0',
    description="Transmogrifier migration for Plone Collective",
    long_description=long_description,
    # Get more from https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Plone",
        "Framework :: Plone :: 5.0",
        "Framework :: Plone :: 5.1",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
    ],
    keywords='Python Plone',
    author='Andreas Jung',
    author_email='info@zopyx.com',
    url='https://pypi.python.org/pypi/collective.plone5migration',
    license='GPL version 2',
    packages=find_packages('src', exclude=['ez_setup']),
    namespace_packages=['collective'],
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        'plone.api>=1.8.4',
        'Products.GenericSetup>=1.8.2',
        'setuptools',
        'z3c.jbot',
        "furl",
        "python-arango",
        "tqdm",
        "pyyaml",
        "requests",
        "attrdict",
        "dateparser",
        "python-magic"
    ],
    extras_require={
        'test': [
            'plone.app.testing',
            # Plone KGS does not use this version, because it would break
            # Remove if your package shall be part of coredev.
            # plone_coredev tests as of 2016-04-01.
            'plone.testing>=5.0.0',
            'plone.app.contenttypes',
            'plone.app.robotframework[debug]',
        ],
    },
    entry_points="""
    [z3c.autoinclude.plugin]
    target = plone
    [console_scripts]
    migration-import = collective.plone5migration.migration.migration_import:main
    import-jsondump-into-arangodb = collective.plone5migration.migration.import_jsondump_into_arangodb:main
    check-images = collective.plone5migration.migration.check_images:main
    read-data= collective.plone5migration.migration.read_data:main
    find-messy-html-documents = collective.plone5migration.migration.find_messy_html_documents:main
    fix-object-ordering-after-migration = collective.plone5migration.migration.fix_object_ordering:main
    """,
)
