import datetime
import logging
from argparse import ArgumentParser
import time

import filetracker


logger = logging.getLogger('ftcachecleanerd')


class CacheCleaner(object):
    """Tool for periodically cleaning cache of the file tracker.
       Designed to work as a daemon.
    """

    def __init__(self, cache_size_limit, cache_dirs=None,
                 scan_interval=datetime.timedelta(minutes=10),
                 percent_cleaning_level=50.0):
        """
        :param iterable cache_dirs: list of paths to
        :class:`filetracker.Client` cache directories
        :param int cache_size_limit: soft limit for logical files size
        :param datetime.timedelta scan_interval: interval specifying how
        often scan disk and optionally clean cache
        :param float percent_cleaning_level: how many percent of
        ``cache_size_limit`` of newest cache files do not delete during
        cleaning cache
        """
        self.clients = [filetracker.Client(cache_dir=dir)
                        for dir in cache_dirs or (None,)]
        self.file_index = []
        self.scan_interval = scan_interval
        self.cache_size_limit = cache_size_limit
        self.cleaning_level = cache_size_limit * percent_cleaning_level / 100.

    def run(self):
        logger.info('Starting daemon.')
        while True:
            self._scan_disk()
            do_cleaning, delete_from_index = self._analyze_file_index()
            if do_cleaning:
                self._clean_cache(delete_from_index)
            sleeping_until_time = datetime.datetime.now() + self.scan_interval
            logger.info('Sleeping until %s.', sleeping_until_time)
            time.sleep(self.scan_interval.total_seconds())

    def _scan_disk(self):
        logger.info('Scanning disk...')
        self.file_index = []
        for client in self.clients:
            self.file_index.extend(client.list_local_files())

    def _analyze_file_index(self):
        logger.info('Analyzing cache...')

        self.file_index.sort(key=lambda x: (-x.mtime, x.size))
        cache_size = 0
        delete_from_index = None
        do_cleaning = False
        for i, fe in enumerate(self.file_index):
            cache_size += fe.size
            if (delete_from_index is None and
                    cache_size >= self.cleaning_level):
                delete_from_index = i
            if cache_size >= self.cache_size_limit:
                do_cleaning = True

        logger.info('Analysis done. Cache size: %s.',
                    format_size_with_unit(cache_size))
        if not do_cleaning:
            logger.info('Decided not to perform cache cleaning.')

        return do_cleaning, delete_from_index

    def _clean_cache(self, delete_from_index):
        # assumption: run after cache analyze
        logger.info('Performing cache cleaning...')
        deleted_files_cnt = 0
        deleted_bytes = 0
        assert self.file_index[0].mtime >= self.file_index[-1].mtime
        for fe in self.file_index[delete_from_index:]:
            logger.debug('Deleting file: %s from store located at: %s',
                         fe.name, fe.client.local_store.dir)
            fe.client.delete_file(fe.name)
            deleted_files_cnt += 1
            deleted_bytes += fe.size
        del self.file_index[delete_from_index:]
        logger.info('Cleaning done. Deleted %d files, total %s.',
                    deleted_files_cnt, format_size_with_unit(deleted_bytes))


def main():
    usage = "usage: %(prog)s [options]"
    parser = ArgumentParser(usage=usage)

    parser.add_argument('-c', '--cache-dirs', dest='cache_dirs', nargs='+',
                        default=[filetracker.Client.DEFAULT_CACHE_DIR],
                        help="Paths to the local cache directories. If not "
                        "specified, uses default File Tracker directory: "
                        "%(default)s")
    parser.add_argument('-s', '--cache-size-limit', dest='cache_size_limit',
                        help="Soft limit for cache. Must be used with the "
                        "following units: B, K, M, G, T and can be chained. "
                        "Example: 1G512M. Note: K=2**10.")
    parser.add_argument('-i', '--scan-interval', dest='scan_interval',
                        default='1h',
                        help="How often performs cache scanning. Must be "
                        "used with the following units: s, m, h, d and can "
                        "be chained. Example: 1h30m [Default: %(default)s]")
    parser.add_argument('-p', '--percent-cleaning-level',
                        dest='percent_cleaning_level',
                        type=float, default=50.0,
                        help="Percent of cache size limit that should be "
                        "*NOT* deleted during cleaning the cache. Newest "
                        "files will remain. [Default: %(default).1f]")
    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument('-d', '--debug', dest='debug', default=False,
                               action='store_true',
                               help="Enables debug logging.")
    logging_group.add_argument('-q', '--quiet', dest='quiet', default=False,
                               action='store_true', help="Disables logging.")

    args = parser.parse_args()
    if not args.cache_size_limit:
        parser.error("Missing --cache-size-limit option. "
                     "Try --help for more details.")

    level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    if args.quiet:
        level = None
    logging.basicConfig(
        format="%(asctime)-23s %(name)s %(levelname)s: %(message)s",
        level=level)

    try:
        scan_interval = parse_time_delta(args.scan_interval)
        cache_size_limit = parse_size(args.cache_size_limit)
    except ValueError as exception:
        parser.error(exception)

    daemon = CacheCleaner(
        cache_size_limit=cache_size_limit,
        cache_dirs=args.cache_dirs,
        scan_interval=scan_interval,
        percent_cleaning_level=args.percent_cleaning_level)
    daemon.run()


_time_units = dict(s=1, m=60, h=60 * 60, d=24 * 60 * 60)
_size_units = dict(B=1, K=2**10, M=2**20, G=2**30, T=2**40)


def parse_time_delta(text):
    seconds = parse_units(text, _time_units)
    return datetime.timedelta(seconds=seconds)


def parse_size(text):
    return parse_units(text, _size_units)


def format_size_with_unit(number):
    return format_with_unit(number, _size_units)


def parse_units(text, units):
    result = 0
    value = ''
    for ch in list(str(text).strip()):
        if ch.isdigit():
            value += ch
        elif ch not in units:
            raise ValueError('Unknown unit "{}" in: {}'.format(ch, text))
        else:
            unit = units[ch]
            if not value:
                raise ValueError('Unit without numeric value: {}'.format(
                    text))
            result += unit * int(value)
            value = ''
    if value:
        raise ValueError('Numeric value without unit: {}'.format(text))
    return result


def format_with_unit(number, size_units):
    units = sorted(size_units.items(), key=lambda x: -x[1])
    for unit, size in units:
        if number >= size:
            return "{amount:.3f}{unit}".format(amount=float(number) / size,
                                               unit=unit)
    return "{amount:.3f}{unit}".format(amount=number, unit=units[-1][0])


if __name__ == '__main__':
    main()
