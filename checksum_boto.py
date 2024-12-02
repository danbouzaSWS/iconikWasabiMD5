import hashlib
import logging
import boto3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import botocore.exceptions
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Wasabi credentials from environment variables
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")
ENDPOINT_URL = os.getenv("WASABI_ENDPOINT_URL", "https://s3.us-east-2.wasabisys.com")  # Default endpoint

# Extensions to skip
SKIP_EXTENSIONS = {".pfk", ".pek", ".cfa", ".mpegindex"}

# Wasabi rate limits (in operations per second)
GET_LIMIT = 16  # Max 1000 GETs per minute


@retry(
    retry=retry_if_exception_type(botocore.exceptions.ClientError),
    stop=stop_after_attempt(5),  # Stop after 5 attempts
    wait=wait_exponential(multiplier=1, min=1, max=16)  # Exponential backoff
)
def rate_limited_get(s3_client, bucket_name, object_key):
    """Rate-limited GET operation with retry and backoff."""
    time.sleep(1 / GET_LIMIT)  # Enforce rate limit
    return s3_client.get_object(Bucket=bucket_name, Key=object_key)


@retry(
    retry=retry_if_exception_type(botocore.exceptions.ClientError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=16),
)
def rate_limited_put(s3_client, bucket_name, object_key, body):
    """Rate-limited PUT operation with retry and backoff."""
    time.sleep(1 / GET_LIMIT)  # Enforce rate limit
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)


def calc_hash_md5(file_stream, file_size):
    """Calculate MD5 checksum for a given file stream."""
    md5 = hashlib.md5()
    with tqdm(
        total=file_size,
        desc="Processing File",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,  # Use KB for better readability
        bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} {unit} [{rate_fmt} | ETA: {remaining}]",
        colour="cyan",  # Set color to cyan
        leave=False,  # Hides this bar when the file is done
    ) as file_pbar:
        while True:
            chunk = file_stream.read(1024 * 1024)  # Read in 1 MB chunks
            if not chunk:
                break
            md5.update(chunk)
            file_pbar.update(len(chunk))
    return md5.hexdigest()


def process_file(object_key, s3_client):
    """Download file stream, calculate its MD5 checksum, and upload it as a .md5 file."""
    try:
        # Check if .md5 file already exists
        md5_key = f"{object_key}.md5"
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=md5_key)
            logging.info(f"Checksum file already exists for {object_key}. Skipping.")
            return
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise

        # Download the file and calculate its checksum
        response = rate_limited_get(s3_client, BUCKET_NAME, object_key)
        file_size = response['ContentLength']  # Get file size for progress bar
        md5_hash = calc_hash_md5(response['Body'], file_size)

        # Upload the checksum as a .md5 file
        rate_limited_put(s3_client, BUCKET_NAME, md5_key, f"{md5_hash}  {object_key}\n")
        logging.info(f"Uploaded checksum for {object_key} to {md5_key}")

    except Exception as e:
        logging.warning(f"Failed to process {object_key}: {e}")


def list_files(prefix, s3_client):
    """List all files in the S3 bucket with optional prefix."""
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        time.sleep(1 / GET_LIMIT)  # Enforce rate limit for each paginated request
        for obj in page.get('Contents', []):
            if not obj['Key'].lower().endswith(tuple(SKIP_EXTENSIONS)):
                yield obj['Key']


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix', default="", help="Path within the Wasabi bucket.")
    parser.add_argument('--debug', action='store_true', help="Enable debug output")
    parser.add_argument('--threads', type=int, default=os.cpu_count(), 
                        help="Number of parallel threads (default: number of CPU cores)")

    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    # Initialize S3 client with credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

    logging.info(f"Listing files in bucket {BUCKET_NAME} with prefix '{args.prefix}'...")
    all_files = list(list_files(args.prefix, s3_client))
    logging.info(f"Found {len(all_files)} files to process.")

    # Overall job progress bar
    with tqdm(
        total=len(all_files),
        desc="Processing Files",
        unit="file",
        bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} {unit} [{rate_fmt} | ETA: {remaining}]",
        colour="green",  # Set color to green
    ) as job_pbar:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = [
                executor.submit(process_file, object_key, s3_client)
                for object_key in all_files
            ]
            for future in futures:
                future.add_done_callback(lambda p: job_pbar.update(1))


if __name__ == '__main__':
    main()
