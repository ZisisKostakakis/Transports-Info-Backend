import sys
import pytest
# import main from generate_csv_data.py
from generate_csv_data import main

# For now its take off as we need to give aws creds to run the tests
# def test_generate_csv_data_aws_pass():
#     # This should pass and creates the csv file and uploads it to S3 and DDB
#     sys.argv = ['main.py', '-g', '1', '-v', '-type', 'test',
#                 '-onaws', '-onddb', '-u', 'webapp', '-o']
#     assert main() is True


def test_generate_csv_data_overwrite_fail():
    # This should fail as the file already exists and overwrite is not enabled
    sys.argv = ['main.py', '-g', '1', '-v', '-type', 'test',
                '-onaws', '-onddb', '-u', 'webapp']
    assert main() is False


def test_generate_csv_data_type_fail():
    # This should fail as the type is wrong
    sys.argv = ['main.py', '-g', '1', '-v', '-type', 'wrong_type',
                '-onaws', '-onddb', '-u', 'webapp', '-o']
    assert main() is False


def test_generate_csv_data_local_pass():
    # This should pass and creates the csv file locally
    sys.argv = ['main.py', '-g', '1', '-v', '-type', 'test', '-o']
    assert main() is True


def test_generate_csv_data_generation_number_fail():
    # This should fail as the generation number is wrong
    sys.argv = ['main.py', '-g', '-1', '-v', '-type', 'test', '-o']
    assert main() is False
    with pytest.raises(ValueError) as excinfo:
        sys.argv = ['main.py', '-g', '1.50', '-v', '-type', 'test', '-o']
        main()
    assert 'invalid literal for int() with base 10: \'1.50\'' in str(excinfo.value)


def test_generate_csv_data_no_args_fail():
    # This should fail as there are no arguments
    with pytest.raises(SystemExit) as excinfo:
        sys.argv = ['main.py']
        main()
    assert excinfo.value.code == 2
