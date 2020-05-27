import boto3
import botocore
import uuid
from boto3 import client
import logging
import os

s3 = boto3.resource('s3')

class BotoS3Res:
    """managing stuff"""
    '''must be a resource object'''
    def __init__(self,s3):
        '''After the user creates an s3 object'''
        self.s3 = s3
    
    def create_bucket_name(self, bucket_prefix):
        '''returns a string that adds random values after the prefix'''
        return "".join([bucket_prefix.lower(), str(uuid.uuid4())])

    def createbucket(self,bucketname,location=None):
        ''' creates bucket object'''
        self.s3.create_bucket(Bucket=bucketname, CreateBucketConfiguration={
                'LocationConstraint': 'us-east-2'
        })

    def printBuckets(self):
        '''Prints all existing buckets'''
        for bucket in s3.buckets.all():
            print(bucket)

    def getBucketNames(self):
        return [i.name for i in s3.buckets.all()]

# class BotoS3Clin:
#     def __init__(self, s3):
#         self.s3 = s3

#     def

    

class s3Bucket:
    def __init__(self, s3 : BotoS3Res ,bucketname :str ):
        self.bucketname = bucketname
        self.s3 = s3

    def getBucket(self):
            bucket = self.s3.Bucket(self.bucketname)
            exists = True
            try:
                s3.meta.client.head_bucket(Bucket=bucketname)
            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    exists = False
                    return "Does not exist boiii"

            return bucket

    def storeDataonBucket(self,file):
        ''' 
        Probably needs some tweaking. Uploads data to the cloud
        '''
        try:
            self.s3.Object(self.bucketname, file).put(Body=open(file, 'rb'))
        except FileNotFoundError:
            #probably should not create an empty file just to upload
            open(file, 'w')
            self.s3.Object(self.bucketname, file).put(Body=open(file, 'rb'))
        finally:
            os.remove(file)
            print("I should close the file")

    def deleteBucket(self):
        '''Probably needs more adjustments'''
        try:
            bucket = self.s3.Bucket(self.bucketname)
            for key in bucket.objects.all():
                key.delete()
            bucket.delete()
            print("Bucket has been deleted")       
        except botocore.exceptions.ClientError:
            print(f"{self.bucketname}This bucket doesn't exist")

    def downloadData(self, tofilename : str, fromfilename :str):
        try:
            bucket = getBucket(self.bucket)
            with open(tofilename, 'wb') as data:
                bucket.download_fileobj(fromfilename, data)
        except botocore.exceptions.ClientError:
            raise Exception("Object does not exist")

    # def UploadDate(self, bucket: str, file:str):
    #     self.s3.Object(bucket, file).put(Body=open(file, 'rb'))

    def deleteFile(self, file: str):
        self.s3.Object(self.bucketname, file).delete()

    def getBucketFiles(self):
        files = s3.Bucket(self.bucketname).Object()
        return files

###for client

class BotoS3Clin:
    def __init__(self,bucket, s3 : boto3.client):
        self.bucket = bucket
        self.s3 = s3

    
    def get_all_s3_keys(self):
        """Get a list of all keys in an S3 bucket."""
        keys = []

        kwargs = {'Bucket': self.bucket}
        while True:
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                keys.append(obj['Key'])

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

        return keys

    def get_s3_keys(self):
        """Get a list of keys in an S3 bucket."""
        keys = []
        resp = self.s3.list_objects_v2(Bucket= self.bucket)
        for obj in resp['Contents']:
            keys.append(obj['Key'])
        return keys


