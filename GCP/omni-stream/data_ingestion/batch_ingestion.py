import apache_beam as beam
import yaml
from apache_beam.options.pipeline_options import PipelineOptions

# Load Config
with open("configs/config.yaml") as f:
    config = yaml.safe_load(f)

BUCKET_NAME = config["gcp"]["bucket_name"]


# Dataflow Pipeline for Batch Processing
def run_batch_pipeline():
    options = PipelineOptions(
        runner="DataflowRunner",
        project=config["gcp"]["project_id"],
        temp_location=f"gs://{BUCKET_NAME}/temp",
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read CSV" >> beam.io.ReadFromText(f"gs://{BUCKET_NAME}/data/*.csv")
            | "Process Data" >> beam.Map(lambda row: row.split(","))
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=f"{config['gcp']['project_id']}:{config['gcp']['bigquery_dataset']}.{config['gcp']['bigquery_table']}",
                schema="field1:STRING, field2:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
