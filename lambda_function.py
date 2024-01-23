import os
import boto3
from PIL import Image

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if key.startswith('tmp/'):
            convert_to_webp(bucket, key)


def convert_to_webp(bucket, key):
    download_path = '/tmp/input'  # Remove the extension to handle any image type
    upload_path = determine_upload_path(key)
    target_path = determine_target_path(key)

    s3_client.download_file(bucket, key, download_path)

    # Create necessary directories
    os.makedirs(os.path.dirname(upload_path), exist_ok=True)

    # Convert image to WebP using PIL
    image = Image.open(download_path)
    image.save(upload_path, 'webp')

    s3_client.upload_file(upload_path, bucket, target_path)

    # Upload the WebP file back to S3
    s3_client.upload_file(upload_path, bucket, upload_path)

    # Clean up local files
    os.remove(download_path)
    os.remove(upload_path)

    # Clean TMP files
    s3_client.delete_object(Bucket=bucket, Key=key)
    s3_client.delete_object(Bucket=bucket, Key=upload_path)


def determine_upload_path(original_key):
    # Determine the upload path based on the original extension
    original_extension = original_key.rsplit('.', 1)[-1]
    upload_path = original_key.rsplit('.', 1)[0] + '.webp'
    return f'/tmp/{upload_path}'

def determine_target_path(original_key):
    # Determine the target path based on the original extension
    original_extension = original_key.rsplit('.', 1)[-1]
    target_path = original_key.replace('tmp/', '')
    return target_path.rsplit('.', 1)[0] + '.webp'