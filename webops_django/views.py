from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import authentication, permissions
from rest_framework import status

from .register import _register
import django_rq
from .ops import compose_graph

class OpsView(APIView):
    """
    
    """
    op_description = ""
    #authentication_classes = (authentication.TokenAuthentication,)
    #permission_classes = (permissions.AllowAny,)

    def get(self, request, format=None):
        """
        Return a list of all ops.
        """
        ops = _register.ops
        out = []
        for op in ops:
            out.append(ops[op].get_meta(request))
        return Response(out)


class AsyncResultView(APIView):

    def get(self, request, job_id):
        queue =  django_rq.get_queue()
        job = queue.fetch_job(job_id)
        if not job:
            return Response(status=status.HTTP_204_NO_CONTENT)

        if job.is_finished:
            if job.status == 'failed':
                return Response(job.exc_info)            
            return Response({ "result" : job.result })



        return Response(status=status.HTTP_204_NO_CONTENT)
    
#todo: move this away..

class OpsGraphView(APIView):
    """
    
    """
    #authentication_classes = (authentication.TokenAuthentication,)
    #permission_classes = (permissions.AllowAny,)

    def post(self, request, format=None):
        out = {}
        op = compose_graph(request.DATA)
        return Response(out)


