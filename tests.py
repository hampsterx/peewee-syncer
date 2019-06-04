import os
import logging
from unittest import TestCase
from peewee import SqliteDatabase, Model, IntegerField
from peewee_syncer.models import SyncManager
from peewee_syncer import get_sync_manager, Processor, LastOffsetQueryIterator

logging.getLogger('peewee').setLevel(logging.INFO)


iteration = 0


class SyncerTests(TestCase):
    """
    Syncer Tests
    """

    def test_sync(self):

        try:
            os.remove('test.db')
        except FileNotFoundError:
            pass

        db = SqliteDatabase('test.db')

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
            def select_by_id(cls, since, limit):
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

        row = SyncManager.get(SyncManager.app == "test")
        print(row.meta)

        def row_output(model):
            return {'id' : model.id, 'value': model.value}

        for x in range(1,5):
            for i in range(1,5):
                TestModel.create(value=x)

        # print(TestModel.select().count())



        def process(it):
            global iteration
            iteration += 1
            for x in it:
                print("it:{} - id:{} - value: {}".format(iteration, x['id'], x['value']))

        def it(since, limit):
            q = TestModel.select_by_id(since, limit=limit)
            return LastOffsetQueryIterator(q.iterator(), row_output_fun=row_output, since_fun=TestModel.get_value, key_fun=TestModel.get_key )

        processor = Processor(
            sync_manager=sync_manager,
            it_function=it,
            process_function=process
        )

        processor.process(limit=16, i=4)

        raise