import sys
import pytest
from get_csv_data import main

# The tests may fail if the csv files are not in the correct location!
def test_csv_data_type_pass() -> None:
    # This should pass as the type is correct
    sys.argv = ['main.py', '-type', 'flights', '-v']
    assert main() is True

# def test_csv_data_type_multiple_pass() -> None:
#     # This should pass as the type is correct
#     sys.argv = ['main.py', '-type', 'flights', 'bus', 'train', '-v', '-onaws', '-u', 'webapp', '-b', 'web-app-python']
#     assert main() is True


def test_csv_data_type_fail() -> None:
    # This should fail as the type is wrong
    sys.argv = ['main.py', '-type', 'wrong_type', '-v']
    assert main() is False


def test_csv_data_no_args_fail() -> None:
    # This should fail as there are no arguments
    with pytest.raises(SystemExit) as excinfo:
        sys.argv = ['main.py']
        main()
    assert excinfo.value.code == 2


# def test_csv_data_on_aws_pass() -> None:
#     # This should pass as the type is correct
#     sys.argv = ['main.py', '-type', 'flights', '-v', '-onaws', '-u', 'webapp', '-b', 'web-app-python']
#     assert main() is True
