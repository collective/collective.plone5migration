# -*- coding: utf-8 -*-
# -*- extra stuff goes here -*-


def initialize(context):
    """Initializer called when used as a Zope 2 product."""


import resource
from  . import patches

try:
    resource.setrlimit(
        resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    )
except:
    import platform

    if platform.system() == "Darwin":
        print(
            ">>>>>>>>>>>>>>>>>>>> Oops setting limit on your Mac <<<<<<<<<<<<<<<<<<<<<<"
        )
    else:
        raise
