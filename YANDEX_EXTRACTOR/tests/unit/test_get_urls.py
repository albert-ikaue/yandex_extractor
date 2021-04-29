import pytest
import sys
import  os

myPath = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, myPath + '/../../src')
from yandex_extractor import date_range


@pytest.mark.parametrize(
    ["expected"],
    [
        # Input 1
        (
            "",


        ),
        # Input 2
        (
            "",

        ),
    ],
)

def test_get_subject(input_subject1,input_subject2, expected):
    """
    :param input_subject1: Keyword
    :param input_subject2: Number of max urls
    :param expected: list of urls
    :return: assertion test accepted
    """

    actual,actual1 = date_range(input_subject1,input_subject2)

    assert type(actual) == list #modify as desired. Dummy example. It could be assert actual == expected
    assert type(actual1) == str

