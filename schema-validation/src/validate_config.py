from asdf.schema import ValidationError, validate
import os
import sys
import yaml

def validate_config(directory_path: str) -> None:
    if not os.path.exists(directory_path):
        print(f'The given filepath {directory_path} does not exist')
        sys.exit(1)

    with open('./config_schema.yaml') as stream:
        config_schema: object = yaml.safe_load(stream.read())

    # All files inside the product folder 'source' directory
    filepaths: os.Path = [os.path.join(directory_path, f) for f in os.listdir(directory_path)]

    for path in filepaths:
        if not os.path.isfile(path):
            print(f'Illegal directory structure found at "{path}", only files are allowed under a product source folder.')
            sys.exit(0)
        with open(path) as stream:
            data = yaml.safe_load(stream.read())

        try:
            validate(data, schema=config_schema)
            print(f'The configuration file "{path}" is valid!')
            sys.exit(0)
        except ValidationError as e:
            print(f'The configuration file "{path}" is invalid:\n\n{e}')
            sys.exit(1)

if __name__ == '__main__':
  [directory_path, *rest] = sys.argv[1:]
  validate_config(directory_path)
