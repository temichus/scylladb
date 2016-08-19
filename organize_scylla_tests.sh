#!/bin/bash
cat scylla_tests | sort | uniq > scylla_tests_tmp
mv scylla_tests_tmp scylla_tests

