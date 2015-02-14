from nose.tools import eq_
from financedatahoarder.services.utils import iter_sort_uniq
from nose_parameterized import parameterized


@parameterized([
    # Each series entry should correspond to last HTTP 200 response of the day
    ([{'url': 'url1', 'value': 23.4},
      {'url': 'url2', 'value': 23.5},
      {'url': 'url1', 'value': 23.1}],
    [{'url': 'url1', 'value': 23.4},
      {'url': 'url2', 'value': 23.5}])
])
def test_parse(values, expected):
    actual = iter_sort_uniq(values, key=lambda entry: entry['url'])
    eq_(expected, list(actual))
