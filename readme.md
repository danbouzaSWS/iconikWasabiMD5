# Wasabi MD5 Checksum Generator

This Python script calculates MD5 checksums for files in a Wasabi bucket. It includes rate-limiting and retry mechanisms to handle Wasabi's API limits.

## Setup

### Prerequisites

- Python 3.8+
- Wasabi account credentials

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/danbouzaSWS/wasabi-checksum.git
   cd wasabi-checksum

2. Create Virtual Enviornment
    python3 -m venv checksum
    source checksum/bin/activate

3. Install dependencies:
    pip install -r requirements.txt

4. Create a .env file:
    WASABI_ACCESS_KEY=your-wasabi-access-key
    WASABI_SECRET_KEY=your-wasabi-secret-key
    WASABI_BUCKET_NAME=your-wasabi-bucket-name
    WASABI_ENDPOINT_URL=https://s3.us-east-2.wasabisys.com

5. Usage
Run the script:
    python checksum_boto.py --prefix some/path/within/bucket
Enable Debugging for Detailed Logs:
    python checksum_boto.py --prefix some/path/within/bucket --debug
Adjust Threads for Speed: Increase the number of threads for faster processing (e.g., 16 threads):
    python checksum_boto.py --prefix some/path/within/bucket --threads 16


