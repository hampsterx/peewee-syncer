import time
import itertools
import logging
from collections import deque

log = logging.getLogger('peewee_syncer')

class LastOffsetQueryIterator:
    def __init__(self, i, row_output_fun, compare_field):
        self.iterator = i
        self.n = 0
        self.row_output_fun = row_output_fun
        self.last_updates = deque([None], maxlen=2)
        self.compare_field = compare_field

    def get_last_offset(self, limit):

        if self.n == limit:
            return self.last_updates[0]
        else:
            return self.last_updates[-1]

    def iterate(self):
        for row in self.iterator:
            self.n = self.n + 1

            value = row[self.compare_field] if isinstance(row, dict) else getattr(row, self.compare_field)
            if self.last_updates[-1] != value:
                self.last_updates.append(value)

            output = self.row_output_fun(row)
            if output:
                yield output


class Processor:
    def __init__(self, sync_manager, it_function, process_function):
        self.it_function = it_function
        self.process_function = process_function
        self.sync_manager = sync_manager


    def process(self, limit, i):

        for n in itertools.count():

            if i > 0 and n == i:
                log.debug("Stopping after iteration {}".format(n))
                break

            last_offset = self.sync_manager.get_last_offset()

            it = self.it_function(since=last_offset['value'], limit=limit)

            self.process_function(it.iterate())

            if self.sync_manager.is_test_run:
                log.debug("Stopping after iteration (test in progress). Processed {} records".format(it.n))
                break

            final_offset = it.get_last_offset(limit=limit)

            if it.n == 0:
                time.sleep(3)
                log.info("Caught up, sleeping..")
            else:
                log.debug("Processed records {}".format("" if final_offset else "unchanged"),
                               extra={'n': it.n, 'offset': final_offset})

                if final_offset != last_offset['value']:
                    if final_offset:
                        self.sync_manager.set_last_offset(final_offset, 0)
                else:
                    time.sleep(3)
                    # todo: if behind current time then abort on second attempt
                    # this would prevent stuck in loop due to bulk updates
                    log.warning("Final offset remains unchaged")

                with self.sync_manager.get_db().connection_context():
                    self.sync_manager.save()

        log.info("Completed importing")

