from asdf.schema import ValidationError, validate
import os
from rich.color import Color
from rich.console import Console
from rich.highlighter import Highlighter
from rich.style import Style
import sys
import textwrap
import yaml

# Highlights all text in red
class ErrorHighlighter(Highlighter):
    def highlight(self, text):
        text.stylize(Style(color=Color.from_rgb(255,0,0)), 0, len(text))

class SuccessHighlighter(Highlighter):
    def highlight(self, text):
        text.stylize(Style(color=Color.from_rgb(0,255,0)), 0, len(text))

# Force terminal so that colours are printed in the github action runner
errorConsole = Console(highlighter=ErrorHighlighter(), force_terminal=True)
console = Console(highlighter=SuccessHighlighter(), force_terminal=True)

def validate_config(environment: str, folder: str, directory_path: str, shared_buckets_path: str) -> None:
    """
    Validate if the config.yaml under the @directory_path is valid.
    Only uses @environment and @folder arguments to print more informative logs.

    Parameters:
        @environment: The dapla team environment the source belongs to i.e. 'uh-varer-prod' or 'play-obr-test'
        @folder: The statistics product folder which contains the configuration file i.e. 'ledstill' or 'sykefra'
        @directory_path: The filepath to the config.yaml file containing 'delomaten' configuration
        @shared_buckets_path: The filepath to the shared-buckets iam.yaml file containing the teams' shared-buckets

    """
    if not os.path.exists(directory_path):
        errorConsole.print(f'\n\nThe given directory path "{directory_path}" does not exist')
        sys.exit(1)

    config_data_path: str = os.path.join(directory_path, 'config.yaml')

    # We expect to find a config.yaml file in the directory
    if not os.path.exists(config_data_path):
        errorConsole.print(
            f'\n\nNo "config.yaml" file exists in the product source folder "{os.path.join(environment, folder)}"\n\n'
        )
        sys.exit(1)

    if not os.path.exists(shared_buckets_path):
        errorConsole.print(f'\n\nThe given shared-buckets path "{shared_buckets_path}" does not exist')
        sys.exit(1)

    with open(config_data_path) as stream:
        config_data: object = yaml.safe_load(stream.read())

    with open('./config_schema.yaml') as stream:
        config_schema: object = yaml.safe_load(stream.read())

    contextual_path = os.path.join(environment, folder, 'config.yaml')
    try:
        validate(config_data, schema=config_schema)
    except ValidationError as e:
        errorConsole.print(f'\n\nThe delomaten configuration file "{contextual_path}" is invalid:\n\n{e}\n\n')
        sys.exit(1)

    with open(shared_buckets_path) as stream:
        shared_buckets_data: object = yaml.safe_load(stream.read())

    # If the shared bucket name specified in the config.yaml doesn't exist in the dapla team, report an error
    if config_data['shared_bucket'] not in shared_buckets_data['buckets'].keys():
        errorConsole.print(textwrap.dedent(
            f"""


            In the configuration file "{contextual_path}" in the field "shared_bucket" the provided bucket "{config_data['shared_bucket']}" does not exist.

            Existing shared buckets for {environment}:
              {"\n".join(['- ' + bucket for bucket in shared_buckets_data['buckets'].keys()])}


            """
        ))
        sys.exit(1)

    if 'output_columns' in config_data and (diff := set(config_data['pseudo']) - set(config_data['output_columns'])) > 0:
        errorConsole.print(textwrap.dedent(
            f"""


            In the configuration file "{contextual_path}" in the field "output_columns" not all columns
            targeted by pseudo operations are listed in the "output_columns".

            The columns that are missing are:
              {"\n".join(['- ' + column for column in diff])}


            """
        ))
        sys.exit(1)

    # TODO: Check that columns in pseudo tasks aren't targeted more than once

    console.print(f'The "{contextual_path}" configuration was successfully validated!')

if __name__ == '__main__':
  [environment, folder, directory_path, shared_buckets_file_path, *rest] = sys.argv[1:]
  validate_config(environment, folder, directory_path, shared_buckets_file_path)
