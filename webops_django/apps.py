from django.apps import AppConfig
from .register import _register
from .ops import compose_graph
from django.conf import settings

import sys
import imp
import importlib
import json
import os

WEBOPS_OPS = getattr(settings, "WEBOPS_OPS", [])
 
class OpsAppConfig(AppConfig):

    name = 'webops_django'
    verbose_name = 'Webops Django'
    loaded = False
 
    def ready(self):
        pass
        
### automatic discovering of webops.py file inside apps.
### similar to what Django admin does. 

if not OpsAppConfig.loaded:

    for app in settings.INSTALLED_APPS:
        # For each app, we need to look for an webops.py inside that app's
        # package. We can't use os.path here -- recall that modules may be
        # imported different ways (think zip files) -- so we need to get
        # the app's __path__ and look for admin.py on that path.

        # Step 1: find out the app's __path__ Import errors here will (and
        # should) bubble up, but a missing __path__ (which is legal, but weird)
        # fails silently -- apps that do weird things with __path__ might
        # need to roll their own admin registration.
        try:
            app_path = importlib.import_module(app).__path__
        except AttributeError:
            continue

        # Step 2: use imp.find_module to find the app's webops.py. For some
        # reason imp.find_module raises ImportError if the app can't be found
        # but doesn't actually try to import the module. So skip this app if
        # its webops.py doesn't exist
        try:
            imp.find_module('webops', app_path)
        except ImportError:
            continue

        # Step 3: import the app's admin file. If this has errors we want them
        # to bubble up.
        importlib.import_module("%s.webops" % app)
        # autodiscover was successful, reset loading flag.

    OpsAppConfig.loaded = True

    for item in WEBOPS_OPS:
        if 'op_class' in item:
            pieces = item['op_class'].split(".")
            cls = pieces[-1]
            module = ".".join(pieces[:-1])
            m = importlib.import_module(module)
            kls = getattr(m, cls)
            _register.register_op(kls)

        elif 'op_graph' in item:
            with open(item['op_graph']) as t:
                data = json.load(t)
                graph = compose_graph(_register, data)
                _register.register_op(graph)




                
                