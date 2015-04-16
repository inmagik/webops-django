from collections import OrderedDict
from django.conf import settings
import warnings
#import logging
#logger = logging.getLogger(__name__)


WEBOPS_BREAK_ON_FAIL_TEST = getattr(settings, "WEBOPS_BREAK_ON_FAIL_TEST", False)


class Register(object):

    def __init__(self):
        self.ops = OrderedDict()

    def register_op(self, op):

        try:
            op.check_op()
            
        except NotImplementedError:
            pass
        except:
            if(WEBOPS_BREAK_ON_FAIL_TEST):
                raise
            #logger.error("Op registration failed, skipping %s" % op.op_name)
            print >>sys.stderr, "!! WEBOPS: Op registration failed, skipping %s" % op.op_id
            return                

        self.ops[op.op_id] = op
    
    def deregister_op(self, op_name):
        pass


    



# register instance
_register = Register()



