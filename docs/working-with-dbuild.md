# Using dbuild scylla in dtest debug mode

Since moving to compile scylla with dbuild, it's a bit more tricky to run dtest with pycharm,
here is the recipe to extract the needed libraries out of the dbuild environment

using idea from
https://stackoverflow.com/a/47115598/459189

```bash
# collect the .so needed inside dbuild
export SCYLLA_DIR=/home/fruch/Projects/scylla-next
export DTEST_DIR=/home/fruch/Projects/scylla-dtest

cd ${SCYLLA_DIR}
./tools/toolchain/dbuild -it -v ${DTEST_DIR}/scripts/dbuild_collect_so.sh:/bin/dbuild_collect_so.sh -- bash
mkdir dynamic_libs_for_dtest
dbuild_collect_so.sh build/release/scylla dynamic_libs_for_dtest/

# outside of the dbuild
# now you can use the directory for running scylla
${SCYLLA_DIR}/dynamic_libs_for_dtest/ld-linux-x86-64.so.2 --library-path ${SCYLLA_DIR}/dynamic_libs_for_dtest ./scylla

# or add it to the pycharm environment variable for running/debugging a dtest
# for usage in pycharm, make sure to use full paths like this:
SCYLLA_DBUILD_SO_DIR=/home/fruch/Projects/scylla-next/dynamic_libs_for_dtest
```
