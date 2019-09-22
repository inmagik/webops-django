import base64
#import imghdr
import uuid
import sys
from django.core.exceptions import ValidationError
from django.core.files.base import ContentFile
from django.utils.translation import ugettext_lazy as _
import json
from rest_framework import serializers
import requests
import logging
log = logging.getLogger('webops_django')

EMPTY_VALUES = (None, '', [], (), {})
 
class FileField(serializers.FileField):
    """
    
    """

    def get_file_data_from_base64(self, data):
        if data['data'].startswith("data"):
            content = data['data'].split(",")[1]
        else:
            content = data['data']
        

        #print "j", content


        # Try to decode the file. Return validation error if it fails.
        try:
            decoded_file = base64.b64decode(content)

        except TypeError:
            raise ValidationError(_("Please upload a valid file."))
        except:
            raise

        # Generate file name:
        file_name = data['filename']
        # Get the file name extension:
        

        file_data = ContentFile(decoded_file, name=file_name)
        return file_data


    def get_file_data_from_url(self, data):
        url = data['data']
        log.info("downloading file: "+ url)
        r = requests.get(url)
        file_name = url.split("/")[-1]
        file_data = ContentFile(r.content, name=file_name)
        log.info("file downloaded: "+ url)
        return file_data



    def to_internal_value(self, data):

        
        if data in EMPTY_VALUES:
            return None
        # Check if this is a base64 string
        if isinstance(data, dict):
            if 'data' not in data:
                raise ValidationError(_("we need the data key!!"))

            if data['data'].startswith('data:'):
                file_data = self.get_file_data_from_base64(data)

            elif data['data'].startswith('http'):
                file_data = self.get_file_data_from_url(data)

            else:
                file_data = self.get_file_data_from_base64(data)

            
            return super(FileField,self).to_internal_value(file_data)
            
        
        else:
            return super(FileField,self).to_internal_value(data)

    


class SingleFileParamsSerializer(serializers.Serializer):
    in_file = FileField(help_text='Input file')
    
    