import os
import logging
import asyncio
from dotenv import load_dotenv
from unittest import TestCase
from peewee_async import MySQLDatabase as AsyncMySQLDatabase, Manager
from peewee import SqliteDatabase, Model, IntegerField
from peewee_syncer import get_sync_manager, Processor, AsyncProcessor, LastOffsetQueryIterator

logging.getLogger('peewee').setLevel(logging.INFO)

# used for async test as cannot support sqlite
load_dotenv()


log = logging.getLogger(__name__)

class BaseTestCase(TestCase):

    def get_sqlite_db(self):
        try:
            os.remove('test.db')
        except FileNotFoundError:
            pass

        return SqliteDatabase('test.db')


class SyncerTests(BaseTestCase):
    """
    Syncer Tests
    """

    def test(self):

        db = self.get_sqlite_db()

        from peewee_syncer.models import SyncManager

        SyncManager.init_db(db)
        SyncManager.create_table()

        class TestModel(Model):

            value = IntegerField()

            @classmethod
            def get_value(cls, item):
                return item.value

            @classmethod
            def get_key(cls, item):
                return item.id

            @classmethod
            def select_since_id(cls, since, limit):
                q = cls.select().where(cls.id > since)

                if limit:
                    q = q.limit(limit)

                return q

            class Meta:
                database = db

        TestModel.create_table()

        sync_manager = get_sync_manager(app="test",
                                        start=0,
                                        test=None
                                        )

        output = []

        def row_output(model):
            data = {'id': model.id, 'value': model.value}
            output.append(data)
            return data

        for i in range(25):
            TestModel.create(id=i + 1, value=i + 1)

        self.assertEqual(25, TestModel.select().count())

        iteration = 0

        def process(it):
            nonlocal iteration
            iteration += 1
            for x in it:
                log.debug("process it={} id={}".format(iteration, x['id']))

        def it(since, limit):
            log.debug("it since={} limit={}".format(since, limit))
            q = TestModel.select_since_id(since, limit=limit)
            return LastOffsetQueryIterator(q.iterator(), row_output_fun=row_output,
                                           key_fun=TestModel.get_key, is_unique_key=True)

        processor = Processor(
            sync_manager=sync_manager,
            it_function=it,
            process_function=process,
            sleep_duration=0
        )

        processor.process(limit=10, i=5)

        self.assertEqual(len(output), 25)

        self.assertEqual(output[0]['id'], 1)
        self.assertEqual(output[-1]['id'], 25)


class AsyncSyncerTests(BaseTestCase):
    """
    Async Syncer Tests
    Note: Not supported with SQLite yet: https://github.com/05bit/peewee-async/issues/126

    """

    def get_mysql_db(self):
        # uses .env via load_dotenv()

        DB_NAME = os.environ.get("DB_NAME", "test")
        DB_HOST = os.environ["DB_HOST"]
        DB_PORT = int(os.environ.get("DB_port", "3306"))
        DB_USERNAME = os.environ["DB_USERNAME"]
        DB_PASSWORD = os.environ["DB_PASSWORD"]

        db = AsyncMySQLDatabase(
            database=DB_NAME,
            user=DB_USERNAME,
            host=DB_HOST,
            port=DB_PORT,
            password=DB_PASSWORD
        )

        return db


    def test(self):

        db = self.get_mysql_db()

        from peewee_syncer.models import SyncManager

        # Init/Create in sync mode
        SyncManager.init_db(db)
        SyncManager.create_table()

        # Clear out from previous test run
        SyncManager.delete().execute()

        sync_manager = get_sync_manager(app="test-async", start=0)

        # Now disable sync
        db.set_allow_sync(False)

        # Fiddle the db for peewee-async to be happy (todo: fixme)
        SyncManager._meta.database = db

        # todo: fixme
        db_object = Manager(db, loop=None)

        def it(since=None, limit=None):

            log.debug("Getting iterator since={} limit={}".format(since, limit))

            def dummy():
                for x in range(since+1, since+limit+1):
                    log.debug("yielded {}".format(x))
                    yield {"x": x}

            return LastOffsetQueryIterator(dummy(), row_output_fun=lambda x:x, key_fun=lambda x:x['x'], is_unique_key=True)

        output = []

        async def process(it):
            nonlocal output
            for item in it:
                output.append(item)
                log.debug("process item: {}".format(item))


        processor = AsyncProcessor(
            sync_manager=sync_manager,
            it_function=it,
            process_function=process,
            object=db_object
        )

        async def consume():
            await processor.process(limit=10, i=3)


        asyncio.get_event_loop().run_until_complete(consume())

        self.assertEqual(len(output), 30)

        #raise
