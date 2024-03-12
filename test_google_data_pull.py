from google.cloud import storage
import pandas as pd
from io import BytesIO

from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv()

# Now you can use the environment variable
service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = storage.Client.from_service_account_json(service_account_path)

# Define the bucket name where the file is located
bucket_name = 'foster-care'

# Define the path to the file within the bucket
blob_name = 'FC Variable Values.xlsx'

# Access the bucket
bucket = client.get_bucket(bucket_name)

# Access the blob (file within the bucket)
blob = bucket.blob(blob_name)

# Download the blob's contents as a file-like object
bytes_stream = BytesIO()
blob.download_to_file(bytes_stream)
bytes_stream.seek(0)  # Go to the beginning of the file-like object

# Use pandas to read the file into a DataFrame
df = pd.read_excel(bytes_stream)

# Now you can work with the DataFrame in pandas
print(df.head())  # For example, this prints the