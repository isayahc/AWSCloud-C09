from BotoS3 import BotoS3Res
from BotoS3 import s3Bucket
from BotoS3 import BotoS3Clin
import boto3
import botocore



def main():
    s3 = boto3.resource('s3')
    s3c = boto3.client('s3')

    

    h = BotoS3Res(s3)
    h.printBuckets()
    bucketName = h.getBucketNames()[0]

    sc = BotoS3Clin(bucketName, s3c)
    print(sc.get_all_s3_keys())

    
    buck = s3Bucket(s3, bucketName)
    #print(buck.getBucketFiles())
 

    

if __name__ == "__main__":
    main()