from asdf.schema import ValidationError, validate # type: ignore
from delomaten_schema_validation import PseudoTask, pseudo_task_columns_uniquely_targeted
import pytest
import yaml

def test_valid_delomaten_config():
    with open('./config_schema.yaml') as stream:
        config_schema = yaml.safe_load(stream.read())

    with open('./test/test_data/valid_data.yaml') as stream:
        data = yaml.safe_load(stream.read())

    try:
        validate(data, schema=config_schema)
        assert True
    except ValidationError as e:
        pytest.fail(f'Validation failed\n{e}')

def test_invalid_delomaten_config():
    with open('./config_schema.yaml') as stream:
        config_schema = yaml.safe_load(stream.read())

    for i in range(1, 6):
        filepath = f'./test/test_data/invalid_data{i}.yaml'
        with open(filepath) as stream:
          data = yaml.safe_load(stream.read())
        try:
            validate(data, schema=config_schema)
            pytest.fail(f'Validation of {filepath} passed, but was expected to fail.')
        except ValidationError:
            assert True

def test_pseudo_task_columns_uniquely_targeted():
    with open('./test/test_data/invalid_pseudo_tasks.yaml') as stream:
        pseudo_tasks: list[PseudoTask] = (
          [PseudoTask(**task) for task in yaml.safe_load(stream.read())]
        )

    with pytest.raises(SystemExit):
        pseudo_task_columns_uniquely_targeted(pseudo_tasks)
