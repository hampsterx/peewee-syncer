import time
import os
import itertools
import logging
import asyncio
import backoff
from peewee import OperationalError
from collections import deque

log = logging.getLogger('peewee_syncer')

PEEWEE_SYNC_BACKOFF_MAX_RETRIES = int(os.environ.get("PEEWEE_SYNC_BACKOFF_MAX_RETRIES", "8"))


class LastOffsetQueryIterator:
    def __init__(self, i, row_output_fun, key_fun, is_unique_key=False):
        self.iterator = i
        self.n = 0
        self.row_output_fun = row_output_fun
        self.last_updates = deque([None], maxlen=2)
        self.key_fun = key_fun
        self.is_unique_key = is_unique_key

    def get_last_offset(self, limit):
        # log.debug("Offsets {} n={} limit={}".format(self.last_updates, self.n, limit))
        if self.n == limit and not self.is_unique_key:
            return self.last_updates[0]
        else:
            return self.last_updates[-1]

    def iterate(self):
        for row in self.iterator:
            self.n = self.n + 1

            value = self.key_fun(row)
            if self.last_updates[-1] != value:
                self.last_updates.append(value)

            output = self.row_output_fun(row)
            if output:
                yield output


class Processor:
    def __init__(self, sync_manager, it_function, process_function, sleep_duration=3):
        self.it_function = it_function
        self.process_function = process_function
        self.sync_manager = sync_manager
        self.sleep_duration = sleep_duration

    @classmethod
    def should_stop(cls, i, n):
        if i > 0 and n == i:
            log.debug("Stopping after iteration {}".format(n))
            return True
        return False

    @backoff.on_exception(backoff.expo,  (OperationalError,), max_tries=PEEWEE_SYNC_BACKOFF_MAX_RETRIES)
    def get_last_offset_and_iterator(self, limit):
        last_offset = self.sync_manager.get_last_offset()

        it = self.it_function(since=last_offset['value'], limit=limit, offset=last_offset['offset'])

        return last_offset, it

    def save(self):
        with self.sync_manager.get_db().connection_context():
            self.sync_manager.save()

    def update_offset(self, it, limit, last_offset):
        final_offset = it.get_last_offset(limit=limit)

        if final_offset and final_offset != last_offset['value']:
            self.sync_manager.set_last_offset(value=final_offset, offset=0)
            return True
        else:
            # ID based, either we got none/some records and therefor offset should have changed
            if it.is_unique_key:
                raise Exception("Aborting Sync. Perhaps your key is not unique?")

            if it.n == limit:
                offset = last_offset['offset'] + limit
                self.sync_manager.set_last_offset(value=last_offset['value'], offset=offset)
                log.warning("Limit reached. Offsetting @ {}".format(offset))
                return True
            else:
                log.debug("Final offset remains unchanged")
                return False

    def process(self, limit, i=0, stop_when_caught_up=False):

        for n in itertools.count():

            if self.should_stop(i=i, n=n):
                break

            last_offset, it = self.get_last_offset_and_iterator(limit=limit)

            if not it:
                break

            self.process_function(it.iterate())

            if self.sync_manager.is_test_run:
                log.debug("Stopping after iteration (test in progress). Processed {} records".format(it.n))
                break

            if it.n == 0:
                if stop_when_caught_up:
                    log.info("Caught up, stopping..")
                    return
                else:
                    log.debug("Caught up, sleeping..")
                    time.sleep(self.sleep_duration)
            else:
                updated = self.update_offset(it=it, limit=limit, last_offset=last_offset)
                if updated:
                    self.save()
                else:
                    if stop_when_caught_up:
                        log.info("No changes, stopping..")
                        return

        log.info("Completed processing")

    def process_until_complete(self, limit):
        return self.process(self, limit=limit, i=0, stop_when_caught_up=True)


class AsyncProcessor(Processor):

    def __init__(self, object, sync_manager, it_function, process_function, sleep_duration=3):
        super().__init__(sync_manager=sync_manager, it_function=it_function, process_function=process_function, sleep_duration=sleep_duration)
        self.object = object

    @backoff.on_exception(backoff.expo, (OperationalError,), max_tries=PEEWEE_SYNC_BACKOFF_MAX_RETRIES)
    async def get_last_offset_and_iterator(self, limit):

        last_offset = self.sync_manager.get_last_offset()

        it = await self.it_function(since=last_offset['value'], limit=limit, offset=last_offset['offset'])

        return last_offset, it

    async def save(self):
        await self.object.update(self.sync_manager)

    async def process(self, limit, i=0, stop_when_caught_up=False):

        for n in itertools.count():

            if self.should_stop(i=i, n=n):
                break

            last_offset, it = await self.get_last_offset_and_iterator(limit=limit)

            if not it:
                break

            await self.process_function(it.iterate())

            if self.sync_manager.is_test_run:
                log.debug("Stopping after iteration (test in progress). Processed {} records".format(it.n))
                break

            if it.n == 0:
                if stop_when_caught_up:
                    log.info("Caught up, stopping..")
                    return
                else:
                    log.info("Caught up, sleeping..")
                    await asyncio.sleep(self.sleep_duration)

            else:
                updated = self.update_offset(it=it, limit=limit, last_offset=last_offset)
                if updated:
                    await self.save()
                else:
                    if stop_when_caught_up:
                        log.info("No changes, stopping..")
                        return

        log.info("Completed importing")

    async def process_until_complete(self, limit):
        return await self.process(self, limit=limit, i=0, stop_when_caught_up=True)
