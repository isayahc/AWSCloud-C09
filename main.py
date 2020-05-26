from BotoS3 import BotoS3
import boto3
import botocore


def main():
    s3 = boto3.resource('s3')
    h = BotoS3(s3)
    print(h.getBucketNames())
    

if __name__ == "__main__":
    main()