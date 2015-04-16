import tempfile
import json

import os
import zipfile
import base64
import mimetypes

def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip.write(os.path.join(root, file), file)
    


def export_file(path):

    with open(path) as f:
        data = base64.b64encode(f.read())

    filename = os.path.basename(path)
    mimes = mimetypes.guess_type(path)
    
    return { 
                'data' : data, 
                'path' : path,
                'filename' : filename,
                'mimetype' : mimes[0]
            }


def write_to_temp(in_file):
    #get it on the tmp
    tmp_src = tempfile.NamedTemporaryFile(suffix=in_file.name, delete=False)
    tmp_src.write(in_file.read())
    tmp_src.close()
    return tmp_src.name
        

def unzip_to_temp(in_file):
    
    tmp_src = tempfile.NamedTemporaryFile(suffix=in_file.name, delete=False)
    tmp_src.write(in_file.read())
    tmp_src.close()

    dst_folder = tempfile.mkdtemp()
    with zipfile.ZipFile(tmp_src.name, "r") as z:
        z.extractall(dst_folder)

    os.unlink(tmp_src.name)
    return [os.path.join(dst_folder, x) for x in os.listdir(dst_folder)]

def zip_to_temp(files):
    tmp_dst = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp_dst.close()
    with zipfile.ZipFile(tmp_dst.name, "w") as z:
        for f in files:
            z.write(f, os.path.basename(f))
    return tmp_dst.name


def make_serializer(name, **kwattrs):
    return type(name, (serializers.Serializer,), dict(**kwattrs))



from rest_framework import serializers
def serializer_from_dict(nm, data):

    fields = {}
    for k in data.keys():
        item = data[k]
        kls = item["serializer_class"]
        params = item["kwargs"]

        field_klass = getattr(serializers, kls)
        fields[k] = field_klass(**params)

    return make_serializer(nm, **fields)




