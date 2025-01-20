from asdf.schema import ValidationError, validate
import os
import sys
import yaml

def validate_config(directory_path: str) -> None:
    if not os.path.exists(directory_path):
        print(f'The given directory path "{directory_path}" does not exist')
        sys.exit(1)

    # We only expect to find a config.yaml file
    config_data_path: str = os.path.join(directory_path, 'config.yaml')

    if not os.path.exists(config_data_path):
        print(f'Could not find the "config.yaml" file at "{directory_path}"')
        sys.exit(1)

    with open(config_data_path) as stream:
        config_data: object = yaml.safe_load(stream.read())

    with open('./config_schema.yaml') as stream:
        config_schema: object = yaml.safe_load(stream.read())

    try:
        validate(config_data, schema=config_schema)
        print(f'The configuration file "{config_data_path}" is valid!')
        sys.exit(0)
    except ValidationError as e:
        print(f'The configuration file "{config_data_path}" is invalid:\n\n{e}')
        sys.exit(1)

if __name__ == '__main__':
  [directory_path, *rest] = sys.argv[1:]
  validate_config(directory_path)
