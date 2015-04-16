import copy

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import authentication, permissions
from rest_framework.exceptions import APIException
from rest_framework import serializers
from .helpers import export_file
from .composition import create_partial_op
from django.conf import settings
import traceback
import django_rq

import register

WEBOPS_ALLOW_ASYNC = getattr(settings, "WEBOPS_ALLOW_ASYNC", False)


def get_async_process(op_id, graph_data, params):
    if op_id and not graph_data:
        klass = register._register.ops[op_id]
        return klass().process(params)

    if graph_data:
        klass = compose_graph(graph_data)
        return klass().process(params)


    raise ValueError("get_async_process needs an op_id or a graph")


BASE_OPS_URL = 'ops'

class BaseOp(APIView):

    #permission_classes = (permissions.AllowAny,)
    op_package = "webops"

    @classmethod
    def check_op(self):
        raise NotImplementedError
    
    @classmethod
    def get_parameters_meta(self):
        out = {}
        if not getattr(self,"parameters_serializer" , None):
            return out
        fields = self.parameters_serializer().get_fields()
        for field in fields:
            out[field] = { 
                'type' : fields[field].__class__.__name__ ,
                'description' : fields[field].help_text,
                'choices' : getattr(fields[field], 'choices', None),
                'required' : getattr(fields[field], 'required', None),
                }

        return out

    
    @classmethod
    def get_meta(self, request):
        meta = { 'id' : self.op_id, 'name' : self.op_name, 'description' : self.op_description , 'package':self.op_package}
        meta['parameters'] = self.get_parameters_meta()
        meta['abs_url'] = request.build_absolute_uri(self.op_id + "/")
        meta['url'] = BASE_OPS_URL + '/' + self.op_id 
        output_descriptor = getattr(self, 'output_descriptor', None)
        if output_descriptor:
            meta['output_descriptor'] = self.output_descriptor.__class__.__name__
        else:
            meta['output_descriptor'] = 'FileData'

        return meta

    


    
    def get_result_async(self, parameters):

        try:
            queue =  django_rq.get_queue()
            graph_data = getattr(self, "graph_data", None)
            job = queue.enqueue(get_async_process, self.op_id, graph_data, parameters)
            return { "job_id" : job.id }
        
        except Exception, e:
            if settings.DEBUG:
                tb = traceback.format_exc()
            else:
                tb = str(e)
            raise APIException(detail=tb)



    def get_result_sync(self, parameters):
        od = getattr(self,"output_descriptor" , None)
        
        if not od or od == 'FileData':
            try:
                out_file = self.process(parameters)
                out_response = export_file(out_file['filename'])
            except Exception, e:
                if settings.DEBUG:
                    tb = traceback.format_exc()
                else:
                    tb = str(e)
                raise APIException(detail=tb)
        else:
            try:
                out_data = self.process(parameters)
                out_response = self.output_descriptor.to_representation(out_data)
            except Exception, e:
                if settings.DEBUG:
                    tb = traceback.format_exc()
                else:
                    tb = str(e)
                raise APIException(detail=tb)
        return out_response


    def get_result(self, parameters, async=False):
        if not async:
            return self.get_result_sync(parameters)

        return self.get_result_async(parameters)
    
    def get(self, request, format=None):
        return Response(self.get_meta(request))


    def post(self, request, format=None):
        if getattr(self,"parameters_serializer" , None):
            parameters = self.parameters_serializer(data=request.data)
            parameters.is_valid(raise_exception=True)
            params = parameters.validated_data
        else:
            params = {}

        
        async = WEBOPS_ALLOW_ASYNC and 'async' in request.data and request.data['async'] == True
        out_response = self.get_result(params, async=async)

        return Response(out_response)





def compose_graph(register, data):
    """
    should return a new composed op...
    """
    partial_ops = {}
    partials  = { }
    ops_inputs = {}

    #TODO:PASS IN RANDOM NAME
    op_id = data["op_id"]
    op_name = data["op_name"] or "GraphOp"
    op_description = data["op_description"] or "GraphOp op_description"

    ops = data["ops"]
    for op in ops:
        #print op
        base_op = register.ops[op["op"]]
        op_label = str(op["label"])
        if "partials" in op:
            o2 = create_partial_op (op_label, base_op, op["partials"])
            partial_ops[op_label] = o2
            partials[op_label] = op["partials"]
            
        else:
            partial_ops[op_label] = base_op

    all_ops = partial_ops.keys()
    for op_label in all_ops:
        ser = partial_ops[op_label].parameters_serializer()
        fields = ser.get_fields()
        for field in fields:
            field_clean = field.replace(":", "-")
            fieldname = "%s:%s" % (op_label, field_clean)
            ops_inputs[fieldname] = fields[field]


    output_candidates = all_ops
    
    deps = {}
    wires = []
    if "wires" in data:
        wires = data["wires"]
    

    for wire in wires:
        target_op_id, target_input = wire["to"].split(":")
        if target_op_id not in deps:
            deps[target_op_id] = []
        #check for circular dep
        if wire["from"] in deps and target_op_id in deps[wire["from"]]:
            raise APIException(detail="Circular dependency between %s and %s" % (target_op, wire["from"]))
        
        deps[target_op_id].append({"name": wire["from"], "target" : target_input})
        del ops_inputs[wire["to"]]

        try:
            output_candidates.remove(wire["from"])
        except:
            pass

    if not len(output_candidates):
        #should be impossible to reach .. unless no ops are posted
        raise APIException(detail="No output candidates remained")

    if len(output_candidates) > 1 and "output_op" not in data:
        raise APIException(detail="Too many output candidates. Please use output_op to specify exactly one")        

    output_op = output_candidates[0]
    
    #we should have 1 output candidate, deps and inputs there.
    
    # let's build the process function
    # it should:
    # - remap serializer inputs
    # - combine process functions.

    def new_process(register, parameters):
        outputs = { }

        def remap_parameters(p2, op_id):

            params = {}
            ins = ops_inputs.keys()
            for fieldname in ins:
                op, name = fieldname.split(":")
                if fieldname in p2:
                    name = name.replace("-", ":")
                    params[name] = p2[fieldname]
            return params


        def process_op(op):
            p = {}
            if op in deps:
                for dep in deps[op]:
                    if dep["name"] not in outputs:
                        out_file = process_op(dep["name"])
                        x = getattr(partial_ops[op], "output_descriptor", None)
                        if x is not None:
                            outputs[dep["name"]] = out_file    
                        else:
                            outputs[dep["name"]] = export_file(out_file['filename'])                        
                        print "ook", dep
                        
                    p[dep["target"]] = outputs[dep["name"]]
            
            op_params = remap_parameters(parameters, op)
            op_params.update(p)

            ser = partial_ops[op].parameters_serializer(data=op_params)
            ser.is_valid(raise_exception=True)
            
            print "processing", op
            out =  partial_ops[op]().process(ser.validated_data)
            print "done", op
            return out
        
        return process_op(output_op)
    
    #let's build a new serializer to validate input
    new_parameters_serializer = type("GraphOpSerializer", (serializers.Serializer,), copy.deepcopy(ops_inputs))
    new_output_descriptor = getattr(partial_ops[output_op], "output_descriptor", None)

    #finally the composed operation
    graph_op = type("GraphOp", 
        (BaseOp,),
        {"process": new_process, "parameters_serializer" : new_parameters_serializer,
        "op_id" : op_id,
        "op_name" : op_name, "op_description": op_description, "output_descriptor" : new_output_descriptor,
        "graph_data" : data }
    )
    
    return graph_op
