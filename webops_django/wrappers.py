from .ops import BaseOp

def wrap_function(id, funk, parameters_serializer, output_descriptor, description=None):
    op_id = id
    op_name = funk.__name__
    op_description = funk.__doc__
    kls_name = "Op" + op_name


    def process(self, params):
        return funk(*params.values())

    attrs = {
        "op_id" : op_id,
        "op_name" : op_name,
        "op_description" : op_description,
        "parameters_serializer" : parameters_serializer,
        "output_descriptor" : output_descriptor,
        "process" : process
    }
    
    return type (kls_name, (BaseOp, ), attrs)
