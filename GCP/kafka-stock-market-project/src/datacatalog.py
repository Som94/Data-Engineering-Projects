import os

from dotenv import load_dotenv
from google.cloud import datacatalog_v1

load_dotenv()

project_id = os.getenv("project_id", "")
location = os.getenv("location", "")
entry_group_id = os.getenv("entry_group_id", "")
entry_id = os.getenv("entry_id", "")
bucket_name = os.getenv("bucket_name", "")

current_dir = os.getcwd()
gcp_credential_path = os.getenv("gcp_credential_path", "")
root_dir = os.path.splitdrive(current_dir)[0] + os.sep
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    root_dir, gcp_credential_path
)

client = datacatalog_v1.DataCatalogClient()

# Create an Entry Group
entry_group = datacatalog_v1.EntryGroup(
    display_name="Stock Market Data Group",
    description="Metadata for Stock Market Data in GCS",
)

entry_group_path = client.entry_group_path(project_id, location, entry_group_id)
client.create_entry_group(
    parent=f"projects/{project_id}/locations/{location}",
    entry_group_id=entry_group_id,
    entry_group=entry_group,
)

# Create an Entry for GCS
entry = datacatalog_v1.Entry(
    display_name="Stock Market Data",
    description="Stock Market JSON files stored in GCS",
    gcs_fileset_spec=datacatalog_v1.GcsFilesetSpec(
        file_patterns=[f"gs://{bucket_name}/*.json"]
    ),
    type_=datacatalog_v1.EntryType.FILESET,
)

client.create_entry(parent=entry_group_path, entry_id=entry_id, entry=entry)
print("Data Catalog Entry Created Successfully!")
