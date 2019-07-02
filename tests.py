import os
import logging
import asyncio
from dotenv import load_dotenv
from unittest import TestCase
from peewee import Proxy
from peewee_async import MySQLDatabase as AsyncMySQLDatabase, Manager
from peewee import SqliteDatabase, Model, IntegerField
from peewee_syncer import SyncManager, get_sync_manager, Processor, AsyncProcessor, LastOffsetQueryIterator

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

        # Re proxy to avoid previous test use
        SyncManager._meta.database = Proxy()

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
            def select_since_id(cls, since, limit, offset):
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

        def it(since, limit, offset):
            log.debug("it since={} limit={} offset={}".format(since, limit, offset))
            q = TestModel.select_since_id(since, limit=limit, offset=offset)
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

    def test_offset_processing(self):

        db = self.get_sqlite_db()

        # Re proxy to avoid previous test use
        SyncManager._meta.database = Proxy()

        SyncManager.init_db(db)

        SyncManager.create_table()

        class TestModel(Model):

            value = IntegerField()

            @classmethod
            def get_value(cls, item):
                return item.value

            @classmethod
            def get_key(cls, item):
                return item.value

            @classmethod
            def select_since_value(cls, since, limit, offset):
                q = cls.select().where(cls.value > since)

                if limit:
                    q = q.limit(limit)

                if offset:
                    q = q.offset(offset)

                log.debug(q.sql())
                return q

            class Meta:
                database = db

        TestModel.create_table()

        sync_manager = get_sync_manager(app="test",
                                        start=-1,
                                        test=None
                                        )

        output = []

        def row_output(model):
            data = {'id': model.id, 'value': model.value}
            output.append(data)
            return data

        # Create 15 regular records
        for i in range(15):
            TestModel.create(value=i)

        # Now add 25 with same value (ie an "hump" that will require "offset" to get over)
        for i in range(25):
            TestModel.create(value=50)

        # And a final few
        for i in range(10):
            TestModel.create(value=51+i)

        self.assertEqual(50, TestModel.select().count())

        iteration = 0

        def process(it):
            nonlocal iteration
            iteration += 1
            for x in it:
                log.debug("process it={} id={} value={}".format(iteration, x['id'], x['value']))

        # Note: is_unique_key=False (ie multiple same value may exist (eg same "lastModified" due to bulk update for example)
        def it(since, limit, offset):
            log.debug("it since={} limit={} offset={}".format(since, limit, offset))
            q = TestModel.select_since_value(since, limit=limit, offset=offset)
            return LastOffsetQueryIterator(q.iterator(), row_output_fun=row_output,
                                           key_fun=TestModel.get_key, is_unique_key=False)

        processor = Processor(
            sync_manager=sync_manager,
            it_function=it,
            process_function=process,
            sleep_duration=0
        )

        processor.process(limit=10, i=10)

        # is_unique_key=False reduces in duplicate values when we hit the offset limit
        # todo: cache to avoid dup values?
        self.assertTrue(len(output), 56)

        value_ids = list(set([x['value'] for x in output]))
        # 0-14, 50, 51-60
        self.assertEquals(value_ids, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60])

        ids = list(set([x['id'] for x in output]))

        self.assertEqual(len(ids), 50)
        self.assertEqual(ids[0], 1)
        self.assertEqual(ids[-1], 50)


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

        # Re proxy to avoid previous test use
        SyncManager._meta.database = Proxy()

        # Init/Create in sync mode
        SyncManager.init_db(db)
        SyncManager.create_table()

        # Clear out from previous test run
        SyncManager.delete().execute()

        sync_manager = get_sync_manager(app="test-async", start=0, db=db, set_async=True)

        async def it(since=None, limit=None, offset=None):

            log.debug("Getting iterator since={} limit={} offset={}".format(since, limit, offset))

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
            object=Manager(db, loop=None)
        )

        async def consume():
            await processor.process(limit=10, i=3)


        asyncio.get_event_loop().run_until_complete(consume())

        self.assertEqual(len(output), 30)

        #raise
    
    def test_offset_processing(self):

        db = self.get_mysql_db()

        # Re proxy to avoid previous test use
        SyncManager._meta.database = Proxy()

        # Init/Create in sync mode
        SyncManager.init_db(db)
        SyncManager.create_table()

        # Clear out from previous test run
        SyncManager.delete().execute()

        sync_manager = get_sync_manager(app="test-async", start=0, db=db, set_async=True)

        # 15 regular, 25 @ 50 (ie the "hump"), 10 afterwards
        items = list(range(15)) + list([50 for _ in range(25)]) + list(range(55, 65))
        items = [{'id': i+1, 'x': x} for i,x in enumerate(items)]

        async def it(since=0, limit=0, offset=0):

            log.debug("Getting iterator since={} limit={} offset={}".format(since, limit, offset))

            def dummy():
                nonlocal items
                nonlocal limit
                nonlocal offset

                for item in items:
                    if item['x'] < since:
                        continue

                    if offset > 0:
                        offset -=1
                        continue

                    limit -= 1
                    if limit < 0:
                        break

                    yield item

            return LastOffsetQueryIterator(dummy(), row_output_fun=lambda x:x, key_fun=lambda x:x['x'], is_unique_key=False)

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
            object=Manager(db, loop=None)
        )

        async def consume():
            await processor.process(limit=10, i=8)

        asyncio.get_event_loop().run_until_complete(consume())

        # todo: cache to avoid dup values?
        self.assertTrue(len(output), 59)

        unique_values = list(set([x['x'] for x in output]))

        self.assertEquals(unique_values,
                          [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 50, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64]
                          )

        ids = list(set([x['id'] for x in output]))

        self.assertEqual(len(ids), 50)
        self.assertEqual(ids[0], 1)
        self.assertEqual(ids[-1], 50)