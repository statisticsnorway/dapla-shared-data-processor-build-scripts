from dapla_pseudo import Depseudonymize, Pseudonymize, Repseudonymize
from dapla_metadata.datasets.core import Datadoc
from datetime import date
import logging
from google.cloud import storage
import io
from pathlib import Path
import polars as pl
import sys
import json
import time
import random
import itertools
from gcsfs import GCSFileSystem

def guard_file_exists(gcs_file_path, max_total_time, base_delay=1, max_delay=60, max_retries=None):
  fs = GCSFileSystem()
  def backoff_delays():
      # Generate delays: base * 2^n, capped at max_delay, with jitter
      for n in itertools.count():
          raw = min(base_delay * (2 ** n), max_delay)
          yield raw * random.uniform(0.5, 1.5)

  def try_action(delays, deadline, attempt=1):
      if fs.exists(gcs_file_path):
          return True
      else:
          now = time.time()
          if now >= deadline or (max_retries is not None and attempt > max_retries):
              raise FileExistsError(f"Could not find file: {gcs_file_path}")
          delay = next(delays)
          remaining = deadline - now
          time.sleep(min(delay, max(0, remaining)))
          return try_action(delays, deadline, attempt + 1)

  deadline = time.time() + max_total_time
  return try_action(backoff_delays(), deadline)

def main(file_path):
    pure_path = Path(file_path.removeprefix("gs://"))
    metadata_document_path = "gs://" + str(Path(pure_path).parent / (Path(pure_path).stem + "__DOC.json"))
    guard_file_exists(metadata_document_path, max_total_time=60 * 5) # 5 minutes timeout
    datadoc = Datadoc(metadata_document_path=metadata_document_path)

    try:
        df = pl.read_parquet(file_path)
    except Exception as e:
        logging.error(f"Failed to read {file_path} from parquet into dataframe\n\n{e}")
        sys.exit(1)

    result = (
      Depseudonymize
        .from_polars(df)
        .with_metadata(datadoc)
        .on_fields("fnr")
        .with_stable_id(custom_key="papis-common-key-1", sid_snapshot_date=str(date.today()), on_map_failure="RETURN_NULL")
        .on_fields("snr")
        .with_stable_id(custom_key="papis-common-key-1", sid_snapshot_date="2000-01-01", on_map_failure="RETURN_NULL")
        .run()
    )
    metrics = json.dumps(result.metadata_details, indent=4)
    metadata = result.datadoc
    logging.info("Metrics metadata %s", metrics)
    final_df = result.to_polars()

    client = storage.Client()
    bucket = client.bucket("ssb-play-obr-data-delt-delomat-deltmappe-prod")
    filename = Path(file_path).name
    filename_metrics = f"{Path(file_path).stem}_METRICS.json"
    filename_metadata = f"{Path(file_path).stem}__DOC.json"
    blob_df = bucket.blob(str(Path("ledstill", filename)))
    blob_metrics = bucket.blob(str(Path("ledstill", filename_metrics)))
    blob_metadata = bucket.blob(str(Path("ledstill", filename_metadata)))

    buffer = io.BytesIO()
    final_df.write_parquet(buffer)
    buffer.seek(0)

    blob_df.upload_from_file(buffer, content_type="application/octet-stream")
    logging.info(f"Result uploaded to deltmappe/ledstill/{filename}")
    blob_metrics.upload_from_string(metrics, content_type="application/json")
    logging.info(f"Metrics uploaded to deltmappe/ledstill/{filename_metrics}")
    blob_metadata.upload_from_string(metadata, content_type="application/json")
    logging.info(f"Metadata uploaded to deltmappe/ledstill/{filename_metadata}")
  