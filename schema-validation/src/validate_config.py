from asdf.schema import ValidationError, validate
import os
import sys
import yaml

def validate_config(environment: str, folder: str, directory_path: str) -> None:
    """
    Validate if the config.yaml under the @directory_path is valid.
    Only uses @environment and @folder arguments to print more informative logs.

    Parameters:
        @environment: The dapla team environment the source belongs to i.e. 'uh-varer-prod' or 'play-obr-test'
        @folder: The statistics product folder which contains the configuration file i.e. 'ledstill' or 'sykefra'
        @directory_path: The absolute directory path inside the GHA runner which contains the config.yaml file

    """
    product_name = 'delomaten' # name is pending

    if not os.path.exists(directory_path):
        print(f'\n\nThe given directory path "{directory_path}" does not exist')
        sys.exit(1)

    config_data_path: str = os.path.join(directory_path, 'config.yaml')

    # We expect to find a config.yaml file in the directory
    if not os.path.exists(config_data_path):
        print(
          f"""\n\n
          No "config.yaml" file exists for the product source "{os.path.join(environment, folder)}".

          The full path being search here is "{directory_path}".
          """
        )
        sys.exit(1)

    with open(config_data_path) as stream:
        config_data: object = yaml.safe_load(stream.read())

    with open('./config_schema.yaml') as stream:
        config_schema: object = yaml.safe_load(stream.read())

    contextual_path = os.path.join(environment, folder, 'config.yaml')
    try:
        validate(config_data, schema=config_schema)
        print(f'\n\nThe {product_name} configuration file "{contextual_path}" is valid!')
        sys.exit(0)
    except ValidationError as e:
        print(f'\n\nThe {product_name} configuration file "{contextual_path}" is invalid:\n\n{e}')
        sys.exit(1)

if __name__ == '__main__':
  [environment, folder, directory_path, *rest] = sys.argv[1:]
  validate_config(environment, folder, directory_path)
