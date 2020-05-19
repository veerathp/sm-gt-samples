import sys
from urllib.parse import urlparse
import boto3
import os
import codecs
import csv
from io import StringIO

def main(args):
     try:
          object_type = sys.argv[1]
          s3_input_path = sys.argv[2]
          s3_manifest_output_path = sys.argv[3]
     except IndexError:
          print ("Usage: " + os.path.basename(__file__) + " <object_type:('image' or 'text')> <input_s3_bucket> <output_s3_bucket>")
          sys.exit(1)

     generate_manifest_file(object_type,s3_input_path,s3_manifest_output_path)

def generate_manifest_file(object_type,s3_input_path,s3_manifest_output_path):
     s3_input_path = urlparse(s3_input_path)
     s3_manifest_output_path = urlparse(s3_manifest_output_path)
     s3 = boto3.client("s3")
     objects_response = s3.list_objects(Bucket=s3_input_path.netloc, Prefix=s3_input_path.path[1:])
     objects_list = parse_response(objects_response)
     print("List of Files:\n" + str(objects_list))

     body = b''
     manifest_file_name = "input_manifest.json"
     if object_type == "image":
          manifest_file_name = "image_input_manifest.json"
          body = create_image_manifest(s3_input_path, objects_list)
     elif object_type == "text":
          manifest_file_name = "text_input_manifest.json"
          body = create_text_manifest(s3_input_path, objects_list)
     else:
          raise Exception("Invalid object type")
     
     try:
          if(body):
               s3.put_object(Bucket=s3_manifest_output_path.netloc, Key="{}/{}".format(s3_manifest_output_path.path[1:], manifest_file_name), Body=body)
     except:
          raise 


def create_image_manifest(s3_input_path, objects_list):
     content_list = []
     for item in objects_list:
        entry = {}
        entry['source-ref'] = "s3://{}/{}".format(s3_input_path.netloc,item)
        content_list.append(entry)

     print("Content:\n" + str(content_list))
     content = "".join(str("{}\n".format(line)) for line in content_list)
     return content

def create_text_manifest(s3_input_path, objects_list):
     s3_resource = boto3.resource('s3')
     content_list = []
     for item in objects_list:
        #Read Text from each file
        bucket = s3_resource.Bucket(s3_input_path.netloc)
        key = "{}".format(item)
        obj = bucket.Object(key=key)
        stream = codecs.getreader('utf-8')(obj.get()['Body'])
        lines = list(csv.DictReader(stream))
        for row in lines:
          for key, value in row.items(): 
               entry = {}
               entry['source'] = value
               content_list.append(entry)

     print("Content:\n" + str(content_list))
     content = "".join(str("{}\n".format(line)) for line in content_list)
     return content

def parse_response(response):
    list=[]
    for content in response['Contents']:
        if (content['Size'] > 0):
            file_name = content['Key']
            list.append(file_name)

    return list

if __name__ == "__main__":
    main(sys.argv)