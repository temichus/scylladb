# coding: utf-8

from dtest import Tester
from unittest import skip


class ScyllaUnsupportedTest(Tester):

    @skip('unimplemented')
    def unsupported_feature_cql_counters(self):
        """ Check that a create table with counter returns an informative error """
        fail

    @skip('unimplemented')
    def unsupported_feature_cql_secondary_index(self):
        """ Check that a create index returns an informative error """
        fail

    @skip('unimplemented')
    def unsupported_feature_cql_user_type(self):
        """ Check that a create type returns an informative error """
        fail

    @skip('unimplemented')
    def unsupported_feature_cql_lwt(self):
        """
        Check that a insert with IF returns an informative error
        Check that a update with IF returns an informative error
        Check that a batch with IF returns an informative error
        """
        fail
