import uuid
from random import randrange

from dtest import Tester
from tools import debug
from assertions import assert_invalid
from cassandra.protocol import InvalidRequest


NUM_OF_BANKS = 100
NUM_OF_ACCOUNTS = 1000
INITIAL_BALANCE = 100000


class LwtBankingTest(Tester):
    def prepare(self, nodes=1):
        if not self.cluster.nodelist():
            self.cluster.populate(nodes)
            self.cluster.start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_schema(session, nodes)

        return session

    def create_schema(self, session, rf):
        debug("Creating schema...")
        self.create_ks(session, "ks1", rf)

        session.execute("""
            CREATE TABLE accounts (
                bic TEXT,
                ban TEXT,
                balance VARINT,
                pending_transfer UUID,
                PRIMARY KEY (bic, ban)
            );           
        """)
        session.execute("""
            CREATE TABLE transactions (
                id UUID,
                src_bic TEXT,
                src_ban TEXT,
                dst_bic TEXT,
                dst_ban TEXT,
                amount VARINT,
                status TEXT,
                PRIMARY KEY (id)
            );
        """)
        session.execute("""
            CREATE TABLE transactions_in_progress (
                id UUID,
                PRIMARY KEY (id)
            );
        """)

    @staticmethod
    def populate(session):
        debug("Populate DB...")
        add_new_account = session.prepare("INSERT INTO accounts (bic, ban, balance) VALUES (?, ?, ?)")
        for bic in range(NUM_OF_BANKS):
            for ban in range(NUM_OF_ACCOUNTS):
                session.execute(add_new_account, (str(bic), str(ban), INITIAL_BALANCE))

    def random_payment(self, session, src_bic, src_ban):
        dst_bic = randrange(NUM_OF_BANKS)
        dst_ban = randrange(NUM_OF_ACCOUNTS)
        amount = randrange(800, 1200)
        return transaction(session, src_bic, src_ban, dst_bic, dst_ban, amount)

    def test_multi_tables_lwt_batch(self):
        session = self.prepare()
        transaction_uuid = uuid.uuid4()
        assert_invalid(session, f"""
            BEGIN BATCH
                INSERT INTO ks1.transactions (id, src_bic, src_ban, dst_bic, dst_ban, amount, status)
                    VALUES ({transaction_uuid}, '1', '1', '1', '2', 100, 'in progress')
                    IF NOT EXISTS
                INSERT INTO ks1.transactions_in_progress (id) 
                    VALUES ({transaction_uuid})
                    IF NOT EXISTS
            APPLY BATCH
        """, "BATCH with conditions cannot span multiple tables", InvalidRequest)

    def test_multi_partition_lwt_batch(self):
        session = self.prepare()
        assert_invalid(session, f"""
            BEGIN BATCH
                UPDATE ks1.accounts
                    SET balance = 99000
                    WHERE bic = '1' AND ban = '1'
                    IF balance >= 100 AND pending_transfer = NULL
                UPDATE ks1.accounts
                    SET balance = 101000
                    WHERE bic = '99' AND ban = '999'
                    IF pending_transfer = NULL
            APPLY BATCH
        """, "BATCH with conditions cannot span multiple partitions", InvalidRequest)

    def test_one_transaction(self):
        session = self.prepare()
        self.populate(session)

        total_before = total_balance(session)
        debug(f"Total: {total_before}")

        assert transaction(session, "1", "1", "1", "2", 1000)

        total_after = total_balance(session)
        debug(f"Total after: {total_after}")

        assert total_before == total_after

    def test_pay_salary(self):
        session = self.prepare()
        self.populate(session)
        session.execute("INSERT INTO ks1.accounts (bic, ban, balance) VALUES ('1', '9999', 1000000000)")

        total_before = total_balance(session)
        debug(f"Total before: {total_before}")

        debug("Make payments...")
        for _ in range(1000):
            assert self.random_payment(session, "1", "9999")

        total_after = total_balance(session)
        debug(f"Total after: {total_after}")

        assert total_before == total_after


def transaction(session, src_bic, src_ban, dst_bic, dst_ban, amount):
    transaction_uuid = uuid.uuid4()

    result = session.execute(f"""            
        INSERT INTO ks1.transactions (id, src_bic, src_ban, dst_bic, dst_ban, amount, status)
            VALUES({transaction_uuid}, '{src_bic}', '{src_ban}', '{dst_bic}', '{dst_ban}', 
                       {amount}, 'in progress')
            IF NOT EXISTS
    """)

    if not result[0].applied:
        debug("Unable to create transaction")
        return False

    result = session.execute(f"""
        INSERT INTO ks1.transactions_in_progress (id) VALUES ({transaction_uuid}) 
            IF NOT EXISTS 
            USING TTL 30
    """)

    if not result[0].applied:
        debug("Unable to add transaction to `in progress' table")
        return False

    if src_bic == dst_bic:
        result = session.execute(f"""
            BEGIN BATCH
                UPDATE ks1.accounts
                    SET pending_transfer = {transaction_uuid}
                    WHERE bic = '{src_bic}' AND ban = '{src_ban}'
                    IF balance >= {amount} AND pending_transfer = NULL
                UPDATE ks1.accounts
                    SET pending_transfer = {transaction_uuid}
                    WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'
                    IF pending_transfer = NULL
            APPLY BATCH
        """)

        if not result[0].applied:
            debug("Unable to start transaction (src and dst are the same partition)")
            return False
    else:
        result = session.execute(f"""
            UPDATE ks1.accounts
                SET pending_transfer = {transaction_uuid}
                WHERE bic = '{src_bic}' AND ban = '{src_ban}'
                IF balance >= {amount} AND pending_transfer = NULL
        """)

        if not result[0].applied:
            debug("Unable to start transaction (src)")
            return False

        result = session.execute(f"""
            UPDATE ks1.accounts
                SET pending_transfer = {transaction_uuid}
                WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'
                IF pending_transfer = NULL
        """)

        if not result[0].applied:
            debug("Unable to start transaction (dst)")
            return False

    src_balance = session.execute(
        f"SELECT balance FROM ks1.accounts WHERE bic = '{src_bic}' AND ban = '{src_ban}'")[0].balance
    dst_balance = session.execute(
        f"SELECT balance FROM ks1.accounts WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'")[0].balance

    src_balance = int(src_balance) - amount
    dst_balance = int(dst_balance) + amount

    if src_bic == dst_bic:
        result = session.execute(f"""
            BEGIN BATCH
                UPDATE ks1.accounts
                    SET pending_transfer = NULL, balance = {src_balance}
                    WHERE bic = '{src_bic}' AND ban = '{src_ban}'
                    IF balance >= {amount} AND pending_transfer != NULL
                UPDATE ks1.accounts
                    SET pending_transfer = NULL, balance = {dst_balance}
                    WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'
                    IF pending_transfer != NULL
            APPLY BATCH
        """)

        if not result[0].applied:
            debug("Unable to finish transaction (src and dst are the same partition)")
            return False
    else:
        result = session.execute(f"""
            UPDATE ks1.accounts
                SET balance = {src_balance}
                WHERE bic = '{src_bic}' AND ban = '{src_ban}'
                IF balance >= {amount} AND pending_transfer != NULL
        """)

        if not result[0].applied:
            debug("Unable to finish transaction (src)")
            return False

        result = session.execute(f"""
            UPDATE ks1.accounts
                SET balance = {dst_balance}
                WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'
                IF pending_transfer != NULL
        """)

        if not result[0].applied:
            debug("Unable to finish transaction (dst)")
            return False

        session.execute(f"""
             BEGIN BATCH
                UPDATE ks1.accounts
                    SET pending_transfer = NULL
                    WHERE bic = '{src_bic}' AND ban = '{src_ban}'
                UPDATE ks1.accounts
                    SET pending_transfer = NULL
                    WHERE bic = '{dst_bic}' AND ban = '{dst_ban}'
            APPLY BATCH               
        """)

    result = session.execute(f"DELETE FROM ks1.transactions_in_progress WHERE id = {transaction_uuid} IF EXISTS")

    if not result[0].applied:
        debug("Unable to remove transaction from `in progress' table")
        return False

    result = \
        session.execute(f"UPDATE ks1.transactions SET status = 'completed' WHERE id = {transaction_uuid} IF EXISTS")

    if not result[0].applied:
        debug("Unable to mark transaction as `completed'")
        return False

    return True


def total_balance(session):
    return int(session.execute("SELECT SUM(balance) FROM ks1.accounts")[0][0])
