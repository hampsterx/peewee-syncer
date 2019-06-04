import sqlite3
import coloredlogs, logging
coloredlogs.install(level='DEBUG', fmt="%(name)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


#print("SQL Lite: {}".format(sqlite3.sqlite_version))

#exit(0)


import os
import logging
from functools import partial
from peewee import SqliteDatabase, Model, CharField
from peewee_syncer.utils import upsert_db_bulk
from peewee_syncer import get_sync_manager, SyncManager, Processor, LastOffsetQueryIterator


log = logging.getLogger(__name__)


try:
    os.remove('test.db')
except FileNotFoundError:
    pass

db = SqliteDatabase('test.db')


SyncManager.init_db(db)

# Run once
SyncManager.create_table()

# A model to sync (could be anything, not peewee specific)
class MyModel(Model):

    name = CharField()

    # Method to compare/track
    @classmethod
    def get_key(cls, item):
        return item.id

    # Method to get records from last offset
    @classmethod
    def select_since_id(cls, since, limit):
        q = cls.select().where(cls.id > since)

        if limit:
            q = q.limit(limit)

        return q

    class Meta:
        database = db


MyModel.create_table()


# Start at zero for first run (otherwise start=None to continue from previous position)
sync_manager = get_sync_manager(app="my-sync-service", start=0)


# A model to sync to (could be anything, not peewee specific)
class MySyncModel(Model):

    some_name = CharField()

    class Meta:
        database = db


# A function to map the output to be synced
def row_output(model):
    return {'id': model.id, 'some_name': model.name}


MySyncModel.create_table()


# Iterator Function
def it(since, limit):
    q = MyModel.select_since_id(since, limit=limit)
    return LastOffsetQueryIterator(q.iterator(),
                                   # Function to convert to output
                                   row_output_fun=row_output,
                                   # Function to check the key of current record we are processing
                                   key_fun=MyModel.get_key,
                                   # The key is unique/atomic (use False if processing time based records as can have many for each key)
                                   is_unique_key=True
                                   )


# Processor
processor = Processor(
            sync_manager=sync_manager,
            it_function=it,
            # A process function (iterates over the iterator)
            process_function=partial(upsert_db_bulk, MySyncModel, preserve=['some_name'], conflict_target='id'),
            # Pause up to 1 seconds on each iteration (percentage of records vs limit processed)
            sleep_duration=1
        )


# Add some records
for i in range(25):
    MyModel.create(id=i, name="test_{}".format(i))

log.info("MySyncModel has {} records".format(MySyncModel.select().count()))

# Run (batch of ten, five iterations. set i=None to run forever)
processor.process(limit=10, i=5)

log.info("MySyncModel has {} records".format(MySyncModel.select().count()))
