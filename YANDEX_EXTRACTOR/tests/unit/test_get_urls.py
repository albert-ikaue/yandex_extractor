import pytest
import sys
import  os
import dateutil

myPath = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, myPath + '/../../src')
from yandex_extractor import date_range

time = date_range()
@pytest.mark.parametrize(
    ["expected"],
    [
        # Input 1
        (

            time,


        )
    ],
)


def test_get_subject(expected):
    """
    :param input_subject1: Keyword
    :param input_subject2: Number of max urls
    :param expected: list of urls
    :return: assertion test accepted
    """

    actual = date_range()

    assert actual == expected #modify as desired. Dummy example. It could be assert actual == expected

