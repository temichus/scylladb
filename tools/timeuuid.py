# A wrapper for TimeUUID values.
# In the future it might be added to the driver: https://github.com/scylladb/python-driver/issues/231,
# but for now we need a custom wrapper to compare TimeUUID values in the same way that Cassandra does.

import uuid


# Represents values of CQL type `timeuuid`.
#
# A `timeuuid` is a standard UUID v1 value. It incorporates timestamp information,
# and the comparison operator compares values primarily based on their timestamp,
# followed by the remaining data.
#
# Cassandra uses a custom comparison algorithm. First it compares the timestamp,
# then it takes the last 64 bits, interprets them as a signed 64 bit integer,
# and compares to the other one.
# For example here the first value is smaller than the second, becasue of signed comparison:
# 00000000-0000-1000-8080-808080808080
# 00000000-0000-1000-0000-000000000000
#
# The code is based on org.apache.cassandra.utils.TimeUUID
# https://github.com/apache/cassandra/blob/ae537abc6494564d7254a2126465522d86b44c1e/src/java/org/apache/cassandra/utils/TimeUUID.java
class TimeUUID(uuid.UUID):

    def __init__(self, *args, **kwargs):
        # It's possible to construct a TimeUUID by passing a UUID value:
        # u = uuid.uuid1()
        # t = TimeUUID(u)
        if len(args) > 0 and isinstance(args[0], uuid.UUID):
            uuid.UUID.__init__(self, bytes=args[0].bytes)
        else:
            uuid.UUID.__init__(self, *args, **kwargs)

    # long uuidTimestamp;
    @property
    def uuid_timestamp(self) -> int:
        return self.time

    # long lsb;
    @property
    def lsb(self) -> int:
        return int.from_bytes(self.bytes[8:16], 'big', signed=True)

    # based on equals()
    def __eq__(self, other):
        if isinstance(other, TimeUUID):
            return self.uuid_timestamp == other.uuid_timestamp and self.lsb == other.lsb

        if isinstance(other, uuid.UUID):
            return self.uuid_timestamp == other.time and self.bytes[8:16] == other.bytes[8:16]

        return NotImplemented

    # based on compareTo()
    def __lt__(self, other):
        if isinstance(other, TimeUUID):
            if self.uuid_timestamp != other.uuid_timestamp:
                return self.uuid_timestamp < other.uuid_timestamp
            else:
                return self.lsb < other.lsb

        return NotImplemented

    def __gt__(self, other):
        return not self <= other

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return not self < other


def test_timeuuid():
    # The first half of test timeuuids
    time_version_vals = [
        '00000000-0000-1000',
        '08080808-0808-1808',
        '08080808-0808-1080',
        '80808080-8080-1080',
        '80808080-8080-1808',
        'f7f7f7f7-f7f7-1f7f',
        'f7f7f7f7-f7f7-17f7',
        '7f7f7f7f-7f7f-1f7f',
        '7f7f7f7f-7f7f-17f7',
        'ffffffff-ffff-1fff',
        'fed35080-0efb-11ee',
        '00000257-0efc-11ee',
        '0000030e-1106-11ee',
        'ff000000-1105-11ee',
        '9684ab8a-1199-11ee',
        '9725a562-1199-11ee'
    ]

    # The second half of test timeuuids
    lsb_vals = [
        '0000-000000000000',
        '8080-808080808080',
        '7f7f-7f7f7f7f7f7f',
        'f7f7-f7f7f7f7f7f7',
        'ffff-ffffffffffff',
        'a1ca-00006490e9a4',
        '9547-00006490e9a6',
        '8572-00006494556c',
        '8f02-00006494556a',
        'bdf4-8c8caab689b2',
        'dead-beefdeadbeef'
    ]

    # Cross product of both halves
    test_vals = [time_version + "-" + lsb_val for time_version in time_version_vals for lsb_val in lsb_vals]

    # Timeuuids in test_vals should be sorted in this order
    expected_order = [
        '00000000-0000-1000-8080-808080808080',
        '00000000-0000-1000-8572-00006494556c',
        '00000000-0000-1000-8f02-00006494556a',
        '00000000-0000-1000-9547-00006490e9a6',
        '00000000-0000-1000-a1ca-00006490e9a4',
        '00000000-0000-1000-bdf4-8c8caab689b2',
        '00000000-0000-1000-dead-beefdeadbeef',
        '00000000-0000-1000-f7f7-f7f7f7f7f7f7',
        '00000000-0000-1000-ffff-ffffffffffff',
        '00000000-0000-1000-0000-000000000000',
        '00000000-0000-1000-7f7f-7f7f7f7f7f7f',
        '08080808-0808-1080-8080-808080808080',
        '08080808-0808-1080-8572-00006494556c',
        '08080808-0808-1080-8f02-00006494556a',
        '08080808-0808-1080-9547-00006490e9a6',
        '08080808-0808-1080-a1ca-00006490e9a4',
        '08080808-0808-1080-bdf4-8c8caab689b2',
        '08080808-0808-1080-dead-beefdeadbeef',
        '08080808-0808-1080-f7f7-f7f7f7f7f7f7',
        '08080808-0808-1080-ffff-ffffffffffff',
        '08080808-0808-1080-0000-000000000000',
        '08080808-0808-1080-7f7f-7f7f7f7f7f7f',
        '80808080-8080-1080-8080-808080808080',
        '80808080-8080-1080-8572-00006494556c',
        '80808080-8080-1080-8f02-00006494556a',
        '80808080-8080-1080-9547-00006490e9a6',
        '80808080-8080-1080-a1ca-00006490e9a4',
        '80808080-8080-1080-bdf4-8c8caab689b2',
        '80808080-8080-1080-dead-beefdeadbeef',
        '80808080-8080-1080-f7f7-f7f7f7f7f7f7',
        '80808080-8080-1080-ffff-ffffffffffff',
        '80808080-8080-1080-0000-000000000000',
        '80808080-8080-1080-7f7f-7f7f7f7f7f7f',
        'fed35080-0efb-11ee-8080-808080808080',
        'fed35080-0efb-11ee-8572-00006494556c',
        'fed35080-0efb-11ee-8f02-00006494556a',
        'fed35080-0efb-11ee-9547-00006490e9a6',
        'fed35080-0efb-11ee-a1ca-00006490e9a4',
        'fed35080-0efb-11ee-bdf4-8c8caab689b2',
        'fed35080-0efb-11ee-dead-beefdeadbeef',
        'fed35080-0efb-11ee-f7f7-f7f7f7f7f7f7',
        'fed35080-0efb-11ee-ffff-ffffffffffff',
        'fed35080-0efb-11ee-0000-000000000000',
        'fed35080-0efb-11ee-7f7f-7f7f7f7f7f7f',
        '00000257-0efc-11ee-8080-808080808080',
        '00000257-0efc-11ee-8572-00006494556c',
        '00000257-0efc-11ee-8f02-00006494556a',
        '00000257-0efc-11ee-9547-00006490e9a6',
        '00000257-0efc-11ee-a1ca-00006490e9a4',
        '00000257-0efc-11ee-bdf4-8c8caab689b2',
        '00000257-0efc-11ee-dead-beefdeadbeef',
        '00000257-0efc-11ee-f7f7-f7f7f7f7f7f7',
        '00000257-0efc-11ee-ffff-ffffffffffff',
        '00000257-0efc-11ee-0000-000000000000',
        '00000257-0efc-11ee-7f7f-7f7f7f7f7f7f',
        'ff000000-1105-11ee-8080-808080808080',
        'ff000000-1105-11ee-8572-00006494556c',
        'ff000000-1105-11ee-8f02-00006494556a',
        'ff000000-1105-11ee-9547-00006490e9a6',
        'ff000000-1105-11ee-a1ca-00006490e9a4',
        'ff000000-1105-11ee-bdf4-8c8caab689b2',
        'ff000000-1105-11ee-dead-beefdeadbeef',
        'ff000000-1105-11ee-f7f7-f7f7f7f7f7f7',
        'ff000000-1105-11ee-ffff-ffffffffffff',
        'ff000000-1105-11ee-0000-000000000000',
        'ff000000-1105-11ee-7f7f-7f7f7f7f7f7f',
        '0000030e-1106-11ee-8080-808080808080',
        '0000030e-1106-11ee-8572-00006494556c',
        '0000030e-1106-11ee-8f02-00006494556a',
        '0000030e-1106-11ee-9547-00006490e9a6',
        '0000030e-1106-11ee-a1ca-00006490e9a4',
        '0000030e-1106-11ee-bdf4-8c8caab689b2',
        '0000030e-1106-11ee-dead-beefdeadbeef',
        '0000030e-1106-11ee-f7f7-f7f7f7f7f7f7',
        '0000030e-1106-11ee-ffff-ffffffffffff',
        '0000030e-1106-11ee-0000-000000000000',
        '0000030e-1106-11ee-7f7f-7f7f7f7f7f7f',
        '9684ab8a-1199-11ee-8080-808080808080',
        '9684ab8a-1199-11ee-8572-00006494556c',
        '9684ab8a-1199-11ee-8f02-00006494556a',
        '9684ab8a-1199-11ee-9547-00006490e9a6',
        '9684ab8a-1199-11ee-a1ca-00006490e9a4',
        '9684ab8a-1199-11ee-bdf4-8c8caab689b2',
        '9684ab8a-1199-11ee-dead-beefdeadbeef',
        '9684ab8a-1199-11ee-f7f7-f7f7f7f7f7f7',
        '9684ab8a-1199-11ee-ffff-ffffffffffff',
        '9684ab8a-1199-11ee-0000-000000000000',
        '9684ab8a-1199-11ee-7f7f-7f7f7f7f7f7f',
        '9725a562-1199-11ee-8080-808080808080',
        '9725a562-1199-11ee-8572-00006494556c',
        '9725a562-1199-11ee-8f02-00006494556a',
        '9725a562-1199-11ee-9547-00006490e9a6',
        '9725a562-1199-11ee-a1ca-00006490e9a4',
        '9725a562-1199-11ee-bdf4-8c8caab689b2',
        '9725a562-1199-11ee-dead-beefdeadbeef',
        '9725a562-1199-11ee-f7f7-f7f7f7f7f7f7',
        '9725a562-1199-11ee-ffff-ffffffffffff',
        '9725a562-1199-11ee-0000-000000000000',
        '9725a562-1199-11ee-7f7f-7f7f7f7f7f7f',
        '7f7f7f7f-7f7f-17f7-8080-808080808080',
        '7f7f7f7f-7f7f-17f7-8572-00006494556c',
        '7f7f7f7f-7f7f-17f7-8f02-00006494556a',
        '7f7f7f7f-7f7f-17f7-9547-00006490e9a6',
        '7f7f7f7f-7f7f-17f7-a1ca-00006490e9a4',
        '7f7f7f7f-7f7f-17f7-bdf4-8c8caab689b2',
        '7f7f7f7f-7f7f-17f7-dead-beefdeadbeef',
        '7f7f7f7f-7f7f-17f7-f7f7-f7f7f7f7f7f7',
        '7f7f7f7f-7f7f-17f7-ffff-ffffffffffff',
        '7f7f7f7f-7f7f-17f7-0000-000000000000',
        '7f7f7f7f-7f7f-17f7-7f7f-7f7f7f7f7f7f',
        'f7f7f7f7-f7f7-17f7-8080-808080808080',
        'f7f7f7f7-f7f7-17f7-8572-00006494556c',
        'f7f7f7f7-f7f7-17f7-8f02-00006494556a',
        'f7f7f7f7-f7f7-17f7-9547-00006490e9a6',
        'f7f7f7f7-f7f7-17f7-a1ca-00006490e9a4',
        'f7f7f7f7-f7f7-17f7-bdf4-8c8caab689b2',
        'f7f7f7f7-f7f7-17f7-dead-beefdeadbeef',
        'f7f7f7f7-f7f7-17f7-f7f7-f7f7f7f7f7f7',
        'f7f7f7f7-f7f7-17f7-ffff-ffffffffffff',
        'f7f7f7f7-f7f7-17f7-0000-000000000000',
        'f7f7f7f7-f7f7-17f7-7f7f-7f7f7f7f7f7f',
        '08080808-0808-1808-8080-808080808080',
        '08080808-0808-1808-8572-00006494556c',
        '08080808-0808-1808-8f02-00006494556a',
        '08080808-0808-1808-9547-00006490e9a6',
        '08080808-0808-1808-a1ca-00006490e9a4',
        '08080808-0808-1808-bdf4-8c8caab689b2',
        '08080808-0808-1808-dead-beefdeadbeef',
        '08080808-0808-1808-f7f7-f7f7f7f7f7f7',
        '08080808-0808-1808-ffff-ffffffffffff',
        '08080808-0808-1808-0000-000000000000',
        '08080808-0808-1808-7f7f-7f7f7f7f7f7f',
        '80808080-8080-1808-8080-808080808080',
        '80808080-8080-1808-8572-00006494556c',
        '80808080-8080-1808-8f02-00006494556a',
        '80808080-8080-1808-9547-00006490e9a6',
        '80808080-8080-1808-a1ca-00006490e9a4',
        '80808080-8080-1808-bdf4-8c8caab689b2',
        '80808080-8080-1808-dead-beefdeadbeef',
        '80808080-8080-1808-f7f7-f7f7f7f7f7f7',
        '80808080-8080-1808-ffff-ffffffffffff',
        '80808080-8080-1808-0000-000000000000',
        '80808080-8080-1808-7f7f-7f7f7f7f7f7f',
        '7f7f7f7f-7f7f-1f7f-8080-808080808080',
        '7f7f7f7f-7f7f-1f7f-8572-00006494556c',
        '7f7f7f7f-7f7f-1f7f-8f02-00006494556a',
        '7f7f7f7f-7f7f-1f7f-9547-00006490e9a6',
        '7f7f7f7f-7f7f-1f7f-a1ca-00006490e9a4',
        '7f7f7f7f-7f7f-1f7f-bdf4-8c8caab689b2',
        '7f7f7f7f-7f7f-1f7f-dead-beefdeadbeef',
        '7f7f7f7f-7f7f-1f7f-f7f7-f7f7f7f7f7f7',
        '7f7f7f7f-7f7f-1f7f-ffff-ffffffffffff',
        '7f7f7f7f-7f7f-1f7f-0000-000000000000',
        '7f7f7f7f-7f7f-1f7f-7f7f-7f7f7f7f7f7f',
        'f7f7f7f7-f7f7-1f7f-8080-808080808080',
        'f7f7f7f7-f7f7-1f7f-8572-00006494556c',
        'f7f7f7f7-f7f7-1f7f-8f02-00006494556a',
        'f7f7f7f7-f7f7-1f7f-9547-00006490e9a6',
        'f7f7f7f7-f7f7-1f7f-a1ca-00006490e9a4',
        'f7f7f7f7-f7f7-1f7f-bdf4-8c8caab689b2',
        'f7f7f7f7-f7f7-1f7f-dead-beefdeadbeef',
        'f7f7f7f7-f7f7-1f7f-f7f7-f7f7f7f7f7f7',
        'f7f7f7f7-f7f7-1f7f-ffff-ffffffffffff',
        'f7f7f7f7-f7f7-1f7f-0000-000000000000',
        'f7f7f7f7-f7f7-1f7f-7f7f-7f7f7f7f7f7f',
        'ffffffff-ffff-1fff-8080-808080808080',
        'ffffffff-ffff-1fff-8572-00006494556c',
        'ffffffff-ffff-1fff-8f02-00006494556a',
        'ffffffff-ffff-1fff-9547-00006490e9a6',
        'ffffffff-ffff-1fff-a1ca-00006490e9a4',
        'ffffffff-ffff-1fff-bdf4-8c8caab689b2',
        'ffffffff-ffff-1fff-dead-beefdeadbeef',
        'ffffffff-ffff-1fff-f7f7-f7f7f7f7f7f7',
        'ffffffff-ffff-1fff-ffff-ffffffffffff',
        'ffffffff-ffff-1fff-0000-000000000000',
        'ffffffff-ffff-1fff-7f7f-7f7f7f7f7f7f'
    ]

    sorted_uuids = sorted([TimeUUID(test_val) for test_val in test_vals])
    sorted_uuids_str = [str(id_val) for id_val in sorted_uuids]

    assert sorted_uuids_str == expected_order
