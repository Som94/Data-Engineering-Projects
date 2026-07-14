import apache_beam as beam
import yaml, json
from apache_beam.options.pipeline_options import PipelineOptions

# Load Config
with open("configs/config.yaml") as f:
    config = yaml.safe_load(f)


def run_beam_pipeline():
    options = PipelineOptions(
        runner="DataflowRunner",
        project=config["gcp"]["project_id"],
        temp_location=f"gs://{config['gcp']['bucket_name']}/temp",
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(topic=config["gcp"]["pubsub_topic"])
            | "Process JSON" >> beam.Map(lambda msg: json.loads(msg))
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=f"{config['gcp']['project_id']}:{config['gcp']['bigquery_dataset']}.{config['gcp']['bigquery_table']}",
                schema="field1:STRING, field2:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
