# instructions for pytest porting

* switch all debug/info/error function calls to logger.debug and such, also create a logger per file, and try to move
  all print from tests steps to be `logger.info`

```python
import logging

logger = logging.getLogger(__name__)
```

* replace test function names to begin with test_

```bash
sed -i 's/def \(.*\)_test(/def test_\1(/' bootstrap_tests.py
```

* class name should be also named with "Test", like "TestMyClassName"


* replace all attr with pytest markers, and make sure they are all snake case (see them defined in `pytest.ini`)

```
sed -i 's|@attr(\(.*\))|@pytest.mark.\1|' bootstrap_tests.py
```

* replace skips with:

```
@pytest.mark.skip
or
@pytest.mark.skipIf
```

and `self.skipTest` with `pytest.skip`

* check test collection is working:

```bash
pytest --scylla-version=branch_4.3:rc1 --collect-only bootstrap_test.py  -vvv
# or with docker
./scripts/run_test.sh --scylla-version=branch_4.3:rc1 --collect-only bootstrap_test.py  -vvv
```

* try to switch from anything used in `tools` to their specific modules under it, if you have something in use
  in `tools`, try moving it to the fitting module under tools

* replace all self.assert* function calls, example

```
self.assertTrue(x, msg="x should be true")
# to
assert x, "x should be true"

self.assertEquals(x, y, msg="x and y should be equal")
# to
assert x == y, "x and y should be equal"
```

* assertRegexpMatches is replaced with tools.pytest_regex.PytestRegex. Usage:

```
self.assertRegexpMatches(str(e), 'Cannot create index on index_values of frozen<')
# to
from tools.assertions import PytestRegex

assert str(e) == PytestRegex('Cannot create index on index_values of frozen<'), 'Not expected error'
```

* try-except with self.fail should replace to:
python
```python
try:
    session = self.get_session(node_idx=1, user='normal', password='wrong')
    session.execute("LIST USERS")
except NoHostAvailable as e:
    assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
    debug("can't get session of node2 with normal user/password")
else:
    self.fail('Session should not be created')

# pytest-format
with pytest.raises(expected_exception=NoHostAvailable) as err:
    session = self.get_session(node_idx=1, user='normal', password='wrong')
    session.execute("LIST USERS")
assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
debug("can't get session of node2 with normal user/password")
```

* try-except with multiple `except` statements and self.fail should replace to (the `match` variable can get regex string):
python
```python
try:
    session = self.get_session(node_idx=1)
    self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
    self.fail("Unauthorized expected")
except NoHostAvailable as e:
    self.assertEqual(str(e), 'NoHostAvailable error')
except Exception as e:
    self.assertEqual(str(e),
                     'Error from server: code=2100 [Unauthorized] message='
                     '"You have to be logged in and not anonymous to perform this request"')

# pytest-format
with pytest.raises(expected_exception=(NoHostAvailable, Exception), match='NoHostAvailable error|Error from server: code=2100 [Unauthorized] message='
                     '"You have to be logged in and not anonymous to perform this request"') as err:
    session = self.get_session(node_idx=1)
    self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
```

It also can be used with dict or list. Example:

```
assert {checked dict} == {'key': PytestRegex(r'\d+')}
```

* parametrized tests to be reimplemented via parametrized fixtures :
python
```python
@attr('dtest-full')
class RangeDeletionTester(Tester):

    def __init__(self, *args, **kwargs):
        super(RangeDeletionTester, self).__init__(*args, **kwargs)
        if hasattr(self, 'compaction_strategy'):
            self.compaction_strategy = self.compaction_strategy
        else:
            self.compaction_strategy = 'LeveledCompactionStrategy'
...
...

strategies = ['SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy']
# SMP value should be according to the monster environment
for strategy in strategies:
    cls_name = ('RangeDeletionTester_with_' + strategy)
    vars()[cls_name] = type(cls_name, (RangeDeletionTester,), {'compaction_strategy': strategy, '__test__': True})

# pytest-format

class TestRangeDeletion(Tester):
    compaction_strategy = None

    @pytest.fixture(
        params=['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'TimeWindowCompactionStrategy'],
        autouse=True)
    def fixture_compaction_strategy(self, request):
        self.compaction_strategy = request.param

```

It also can be used with dict or list. Example:

```
assert {checked dict} == {'key': PytestRegex(r'\d+')}
```

* assert functions from `assertions.py` are moved to `tools.assertions`

* Functions `create_ks`, `create_cf`, `create_index`, `create_local_index` are moved to dtest_class and should be
  imported. Replace:

```
self.create_ks(...)

#to

from dtest_class import create_ks

create_ks(...)
```

* For flaky tests use pytest's `flaky` decorator:

```
from flaky import flaky
```

* retrying decorator is moved to `tools.retrying`

* new_node is moved to `tools.cluster`

* Replace `@scylla_mode(...)` with `@pytest.mark.scylla_mode(...)`

* Replace `@require(...)` with `@pytest.mark.require(...)`

* Add converted test name to `.pre-commit-config.yaml`
