import boto3
import datetime
import subprocess

filelist = []

S3_bucket = 'yieldify-aa-stream-dev'

s3 = boto3.resource('s3')

for el in s3.Bucket(S3_bucket).objects.filter(Prefix='v1/json/2018/12/10/2018-12-10-12_1544443800/'):
    filelist.append(el.key)

def f(el):
    S3_bucket = 'yieldify-aa-stream-dev'
    s3 = boto3.resource('s3')
    s3.Bucket(S3_bucket).download_file(el, 'mattia.lzo')
    subprocess.call(['lzop', '-d', 'mattia.lzo'])

    try:
        with open('mattia', 'rb') as f:
            return f.read()
    except:
        print('file missing')

datetime.datetime.now()
sc.parallelize(filelist).map(f).collect()
datetime.datetime.now()






