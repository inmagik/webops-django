from rest_framework.views import APIView
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework import authentication, permissions
from rest_framework import serializers

import copy

def create_partial_serializer(name, base_serializer_class, partials):
    
    ser = base_serializer_class()
    fields = ser.get_fields()
    for p in partials:
        if p in fields:
            del fields[p]

    return type(name, (serializers.Serializer,), copy.deepcopy(fields) )
    


def create_partial_op(name, op_class, partials):
    
    def process_with_partials(self, parameters):
        parameters.update(partials)
        return op_class.process(self, parameters)

    partial_serializer = create_partial_serializer(name + "ParametersSerializer", op_class.parameters_serializer, partials)

    newclass = type(name, (op_class,),
        {"process": process_with_partials, "parameters_serializer" : partial_serializer })
    
    return newclass






    

