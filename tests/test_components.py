import pytest

from wallaby_hires import parset_mixin

given = pytest.mark.parametrize

def test_parset_mixin():
    d0 = {'a':{'value':1}, 'b':{'value':2}}
    d1 = {'b':'Hello', 'c': 'World'}
    expected = 'a=1\nb=Hello\nc=World'
    yanda_ps = parset_mixin(d0, d1).decode('utf-8')
    assert yanda_ps == expected

