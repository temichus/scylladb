# instructions for pytest porting

* switch all debug/info/error function calls to logger.debug and such,
  also create a logger per file, and try to move all print from tests steps to be `logger.info`

```python
import logging
logger = logging.getLogger(__name__)
```

* replace test function names to begin with test_
```bash
sed -i 's/def \(.*\)_test(/def test_\1(/' bootstrap_tests.py
```

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

* try to switch from anything used in `tools` to their specific modules under it,
  if you have something in use in `tools`, try moving it to the fitting module under tools

* replace all self.assert* function calls, example
```python
self.assertTrue(x, msg="x should be true")
# to
assert x, "x should be true"

self.assertEquals(x, y, msg="x and y should be equal")
# to
assert x == y, "x and y should be equal"
```
