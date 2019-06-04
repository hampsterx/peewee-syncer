import os
import logging
import logging
from unittest import TestCase
from peewee import SqliteDatabase, Model, IntegerField
from peewee_syncer.models import SyncManager
from peewee_syncer import get_sync_manager, Processor, LastOffsetQueryIterator

logging.getLogger('peewee').setLevel(logging.INFO)


log = logging.getLogger(__name__)


class SyncerTests(TestCase):
    """
    Syncer Tests
    """

    def get_test_db(self):
        try:
            os.remove('test.db')
        except FileNotFoundError:
            pass

        return SqliteDatabase('test.db')

    def test_sync(self):

        db = self.get_test_db()

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
