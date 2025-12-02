from dapla_pseudo import Depseudonymize, Pseudonymize # noqa
from dapla_metadata.datasets.core import Datadoc
from dapla_metadata.datasets.utility.utils import VariableType
from datetime import date # noqa
import logging
from typing import Any
from pathlib import Path
from pprint import pformat
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
              error_msg = f"Could not find file: {gcs_file_path}"
              logging.error(error_msg)
              raise FileExistsError(error_msg)
          delay = next(delays)
          remaining = deadline - now
          time.sleep(min(delay, max(0, remaining)))
          return try_action(delays, deadline, attempt + 1)

  deadline = time.time() + max_total_time
  return try_action(backoff_delays(), deadline)

def get_decryption_algorithm(variable: VariableType)-> tuple[str, dict[str,str | None]]:
    """Given a pseudonymized variable, update the depseudonymization builder with the correct algorithm and arguments."""
    pseudo_metadata = variable.pseudonymization
    encryption_key_reference = pseudo_metadata.encryption_key_reference

    if pseudo_metadata.encryption_algorithm == 'TINK-DAEAD':
        return ("default_encryption", {"custom_key": encryption_key_reference})
    elif pseudo_metadata.encryption_algorithm == 'TINK-FPE':
        if pseudo_metadata.stable_identifier_type == 'FREG_SNR' and pseudo_metadata.stable_identifier_version is not None:
            failure_strategy: str | None = next((param["failureStrategy"] for param in pseudo_metadata.encryption_algorithm_parameters if "failureStrategy" in param), None)
            return("stable_id", {"custom_key": encryption_key_reference, "sid_snapshot_date": pseudo_metadata.stable_identifier_version, "on_map_failure": failure_strategy})
        elif pseudo_metadata.stable_identifier_type is None and pseudo_metadata.stable_identifier_version is None:
            return("papis_compatible_encryption", {"custom_key": encryption_key_reference})

    raise ValueError(
        f"""Cannot determine depseudonymization algorithm used for variable '{variable.short_name}'. Relevant metadata from the 'pseudonymization' field:

        {pformat(pseudo_metadata)}
        """
    )

def build_and_run_depseudo(df: pl.DataFrame, datadoc_metadata: Datadoc, columns: list[str]) -> Any:
    """Create and then execute a depseudonymization builder block based on the datadoc metadata."""
    variable_dict: dict[str, VariableType] = (
      {v.short_name:v for v in datadoc_metadata.datadoc_model().datadoc.variables}
    )
    variable_names = list(variable_dict.keys())
    for column in columns:
        if column not in variable_names:
            raise ValueError(
              f"Column '{column}' not found in supplied Datadoc metadata variables {variable_names}"
            )

    builder = Depseudonymize.from_polars(df).with_metadata(datadoc_metadata)

    # Dynamically build the 'Depseudonymize' block based on the datadoc pseudonymization metadata
    for column in columns:
        builder = builder.on_fields(column)
        match get_decryption_algorithm(variable_dict[column]):
            case ("default_encryption", kwargs):
                builder = builder.with_default_encryption(**kwargs)
            case ("stable_id", kwargs):
                builder = builder.with_stable_id(**kwargs)
            case ("papis_compatible_encryption", kwargs):
                builder = builder.with_papis_compatible_encryption(**kwargs)
            case default:
                raise ValueError(f"Unexpected match case when building depseudo block\n{default}")

    return builder.run()

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
      Pseudonymize
        .from_polars(df)
        .with_metadata(datadoc)
        .on_fields("fnr")
        .with_stable_id(custom_key="papis-common-key-1", sid_snapshot_date=str(date.today()), on_map_failure="RETURN_NULL")
        .on_fields("snr")
        .with_papis_compatible_encryption(custom_key="papis-common-key-1")
        .on_fields("fornavn","etternavn")
        .with_default_encryption(custom_key="ssb-common-key-2")
        .run()
    )
    metrics = json.dumps(result.metadata_details, indent=2)
    metadata = json.dumps(json.loads(result.datadoc), indent=2)
    logging.info("Metrics metadata %s", metrics)
    final_df = result.to_polars()

    fs = GCSFileSystem()
    filename = Path(file_path).name
    filename_metrics = f"{Path(file_path).stem}_METRICS.json"
    filename_metadata = f"{Path(file_path).stem}__DOC.json"
    output_path = "ssb-dapla-team-data-delt-forbruk-prod/forbruk"

    with fs.open(path=Path(output_path, filename),mode='w') as fh:
        final_df.write_parquet(fh)
        logging.info(f"Result uploaded to forbruk/forbruk/{filename}")

    with fs.open(path=Path(output_path, filename_metrics),mode='w') as fh:
        fh.write(metrics)
        logging.info(f"Metrics uploaded to forbruk/forbruk/{filename_metrics}")

    with fs.open(path=Path(output_path, filename_metadata),mode='w') as fh:
        fh.write(metadata)
        logging.info(f"Metadata uploaded to forbruk/forbruk/{filename_metadata}")
