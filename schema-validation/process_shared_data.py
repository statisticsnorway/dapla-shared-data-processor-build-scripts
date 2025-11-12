from dapla_pseudo import Depseudonymize, Pseudonymize, Repseudonymize
from datetime import date
import logging
from google.cloud import storage
import io
from pathlib import Path
import polars as pl
import sys
import json

def main(file_path):
    try:
        df = pl.read_parquet(file_path)
    except Exception as e:
        logging.error(f"Failed to read {file_path} from parquet into dataframe\n\n{e}")
        sys.exit(1)

    result = (
      Depseudonymize
        .from_polars(df)
        .on_fields("bolig")
        .with_default_encryption(custom_key="ssb-common-key-2")
        .run()
    )

    result = (
      Pseudonymize
        .from_result(result)
        .on_fields("fornavn","etternavn")
        .with_default_encryption(custom_key="ssb-common-key-1")
        .on_fields("fnr","snr")
        .with_stable_id(custom_key="papis-common-key-1", sid_snapshot_date="2024-03-27", on_map_failure="RETURN_NULL")
        .run()
    )
    metrics = json.dumps(result.metadata_details, indent=4)
    logging.info("Metrics metadata %s", metrics)
    final_df = result.to_polars()

    client = storage.Client()
    bucket = client.bucket("ssb-play-obr-data-delt-ledstill-prod")
    filename = Path(file_path).name
    filename_metrics = f"{Path(file_path).stem}_METRICS.json"
    blob_df = bucket.blob(str(Path("ledstill", filename)))
    blob_metrics = bucket.blob(str(Path("ledstill", filename_metrics)))

    buffer = io.BytesIO()
    final_df.write_parquet(buffer)
    buffer.seek(0)

    blob_df.upload_from_file(buffer, content_type="application/octet-stream")
    logging.info(f"Result uploaded to ssb-play-obr-data-delt-ledstill-prod/ledstill/{filename}")
    blob_metrics.upload_from_string(metrics, content_type="application/json")
    logging.info(f"Metrics uploaded to ssb-play-obr-data-delt-ledstill-prod/ledstill/{filename_metrics}")
  