# Peewee Syncer

 [![PyPI version](https://badge.fury.io/py/peewee-syncer.svg)](https://badge.fury.io/py/peewee-syncer) [![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/) [![Python 3.6](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)


Tiny tool to help syncronize data using peewee db model for state persistance.

## Install

```
pip install peewee-syncer
```

## Usage

Note example below uses `upsert_db_bulk()` helper.
This requires sqlite 3.25+ for Upsert support * 

Eg 

Download: https://packages.debian.org/search?keywords=libsqlite3-0

```
 sudo dkpk -i libsqlite3-0_3.27.2-2~bpo9+1_amd64.deb 
```

```
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

```

Output

```
peewee DEBUG ('CREATE TABLE IF NOT EXISTS "sync_manager" ("app" VARCHAR(256) NOT NULL PRIMARY KEY, "meta" TEXT NOT NULL, "modified" DATETIME)', [])
peewee DEBUG ('CREATE TABLE IF NOT EXISTS "mymodel" ("id" INTEGER NOT NULL PRIMARY KEY, "name" VARCHAR(255) NOT NULL)', [])
peewee DEBUG ('SELECT "t1"."app", "t1"."meta", "t1"."modified" FROM "sync_manager" AS "t1" WHERE ("t1"."app" = ?) LIMIT ? OFFSET ?', ['my-sync-service', 1, 0])
peewee DEBUG ('BEGIN', None)
peewee DEBUG ('INSERT INTO "sync_manager" ("app", "meta", "modified") VALUES (?, ?, ?)', ['my-sync-service', '{}', datetime.datetime(2019, 6, 4, 16, 10, 18, 603755)])
peewee DEBUG ('UPDATE "sync_manager" SET "meta" = ?, "modified" = ? WHERE ("sync_manager"."app" = ?)', ['{"value": 0, "type": null, "offset": 0}', datetime.datetime(2019, 6, 4, 16, 10, 18, 609370), 'my-sync-service'])
peewee DEBUG ('CREATE TABLE IF NOT EXISTS "mysyncmodel" ("id" INTEGER NOT NULL PRIMARY KEY, "some_name" VARCHAR(255) NOT NULL)', [])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [0, 'test_0'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [1, 'test_1'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [2, 'test_2'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [3, 'test_3'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [4, 'test_4'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [5, 'test_5'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [6, 'test_6'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [7, 'test_7'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [8, 'test_8'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [9, 'test_9'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [10, 'test_10'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [11, 'test_11'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [12, 'test_12'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [13, 'test_13'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [14, 'test_14'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [15, 'test_15'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [16, 'test_16'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [17, 'test_17'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [18, 'test_18'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [19, 'test_19'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [20, 'test_20'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [21, 'test_21'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [22, 'test_22'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [23, 'test_23'])
peewee DEBUG ('INSERT INTO "mymodel" ("id", "name") VALUES (?, ?)', [24, 'test_24'])
peewee DEBUG ('SELECT COUNT(1) FROM (SELECT 1 FROM "mysyncmodel" AS "t1") AS "_wrapped"', [])
__main__ INFO MySyncModel has 0 records
peewee DEBUG ('SELECT "t1"."id", "t1"."name" FROM "mymodel" AS "t1" WHERE ("t1"."id" > ?) LIMIT ?', [0, 10])
peewee DEBUG ('INSERT INTO "mysyncmodel" ("id", "some_name") VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?) ON CONFLICT ("id") DO UPDATE SET "some_name" = EXCLUDED."some_name"', [1, 'test_1', 2, 'test_2', 3, 'test_3', 4, 'test_4', 5, 'test_5', 6, 'test_6', 7, 'test_7', 8, 'test_8', 9, 'test_9', 10, 'test_10'])
peewee_syncer DEBUG Processed records n=10 offset=10
peewee DEBUG ('UPDATE "sync_manager" SET "meta" = ?, "modified" = ? WHERE ("sync_manager"."app" = ?)', ['{"value": 10, "type": null, "offset": 0}', datetime.datetime(2019, 6, 4, 16, 10, 18, 837385), 'my-sync-service'])
peewee DEBUG ('SELECT "t1"."id", "t1"."name" FROM "mymodel" AS "t1" WHERE ("t1"."id" > ?) LIMIT ?', [10, 10])
peewee DEBUG ('INSERT INTO "mysyncmodel" ("id", "some_name") VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?) ON CONFLICT ("id") DO UPDATE SET "some_name" = EXCLUDED."some_name"', [11, 'test_11', 12, 'test_12', 13, 'test_13', 14, 'test_14', 15, 'test_15', 16, 'test_16', 17, 'test_17', 18, 'test_18', 19, 'test_19', 20, 'test_20'])
peewee_syncer DEBUG Processed records n=10 offset=20
peewee DEBUG ('UPDATE "sync_manager" SET "meta" = ?, "modified" = ? WHERE ("sync_manager"."app" = ?)', ['{"value": 20, "type": null, "offset": 0}', datetime.datetime(2019, 6, 4, 16, 10, 18, 855367), 'my-sync-service'])
peewee DEBUG ('SELECT "t1"."id", "t1"."name" FROM "mymodel" AS "t1" WHERE ("t1"."id" > ?) LIMIT ?', [20, 10])
peewee DEBUG ('INSERT INTO "mysyncmodel" ("id", "some_name") VALUES (?, ?), (?, ?), (?, ?), (?, ?) ON CONFLICT ("id") DO UPDATE SET "some_name" = EXCLUDED."some_name"', [21, 'test_21', 22, 'test_22', 23, 'test_23', 24, 'test_24'])
peewee_syncer DEBUG Processed records n=4 offset=24
peewee DEBUG ('UPDATE "sync_manager" SET "meta" = ?, "modified" = ? WHERE ("sync_manager"."app" = ?)', ['{"value": 24, "type": null, "offset": 0}', datetime.datetime(2019, 6, 4, 16, 10, 18, 874314), 'my-sync-service'])
peewee DEBUG ('SELECT "t1"."id", "t1"."name" FROM "mymodel" AS "t1" WHERE ("t1"."id" > ?) LIMIT ?', [24, 10])
peewee_syncer DEBUG Caught up, sleeping..
peewee DEBUG ('SELECT "t1"."id", "t1"."name" FROM "mymodel" AS "t1" WHERE ("t1"."id" > ?) LIMIT ?', [24, 10])
peewee_syncer DEBUG Caught up, sleeping..
peewee_syncer DEBUG Stopping after iteration 5
peewee_syncer INFO Completed processing
peewee DEBUG ('SELECT COUNT(1) FROM (SELECT 1 FROM "mysyncmodel" AS "t1") AS "_wrapped"', [])
__main__ INFO MySyncModel has 24 records


```