import boto3
import botocore
import uuid
from boto3 import client
import logging
import os

s3 = boto3.resource('s3')

class BotoS3:
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
    
    def getBucket(self,bucketname):
        bucket = self.s3.Bucket(bucketname)
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

    def storeDataonBucket(self,bucketname, file):
        ''' 
        Probably needs some tweaking. Uploads data to the cloud
        '''
        self.s3.Object(bucketname, file).put(Body=open(file, 'rb'))
        # try:
        #     file.close()


    def deleteBucket(self, bucketname):
        '''Probably needs more adjustments'''
        try:
            bucket = self.s3.Bucket(bucketname)
            for key in bucket.objects.all():
                key.delete()
            bucket.delete()
            print("Bucket has been deleted")       
        except botocore.exceptions.ClientError:
            print(f"{bucketname}This bucket doesn't exist")

    def downloadData(self, bucket : str,tofilename : str,fromfilename :str):
        try:
            bucket = getBucket(bucket)
            with open(tofilename, 'wb') as data:
                bucket.download_fileobj(fromfilename, data)
        except botocore.exceptions.ClientError:
            raise Exception("Object does not exist")

    def UploadDate(self, bucket: str, file:str):
        self.s3.Object(bucket, file).put(Body=open(file, 'rb'))

