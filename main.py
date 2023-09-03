# encoding: utf-8
'''
The MIT License (MIT)
Copyright © 2023 Chris Carl <chris.carl@intel.com>
Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the “Software”), to
  deal in the Software without restriction, including without limitation the
  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
  sell copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.

Author:     Chris Carl <chrisbcarl@outlook.com>
Date:       2023-07-05
Modified:   2023-07-05

Modified:
    2023-07-05 - chrisbcarl@outlook.com - init commit, behaves like sequential write/readback, thats it.
'''

from __future__ import print_function, division
import os
import uuid
import random
import tempfile
import shutil
import argparse
import datetime

MEGABYTE = 1 * 1024 * 1024
CACHE_FOLDER = os.path.abspath(os.path.join(os.getcwd(), '.generated'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', type=str)
    parser.add_argument('--time', type=int, default=-1, help='length of time to execute in seconds, else infinite')
    parser.add_argument('--mb', type=int, default=10, help='megabytes to write')
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='if set, overwrite a single file instead of constantly filling the disk'
    )

    args = parser.parse_args()
    dirpath = os.path.abspath(os.path.join(args.path, 'sequential-write'))
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    if not os.path.isdir(CACHE_FOLDER):
        os.makedirs(CACHE_FOLDER)

    print('starting')
    print('writing', args.mb, 'mb to', dirpath, end=' ')
    print('w/{} replacement'.format('' if args.overwrite else 'o'), end=' ')
    print('for {}'.format('infinity' if args.time == -1 else '{} seconds'.format(args.time)), end='\n')

    print('generating a random file of {}mb'.format(args.mb))
    reads, writes = [], []
    try:
        data_filepath = os.path.join(CACHE_FOLDER, '{}mb.data'.format(args.mb * MEGABYTE))
        if os.path.isfile(data_filepath):
            print('using previously generated file "{}"'.format(data_filepath))
            with open(data_filepath, 'rb') as rb:
                data = rb.read()
        else:
            data = bytearray(random.randint(0, 127) for _ in range(args.mb * MEGABYTE))
            # tmp_filepath = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
            print('writing generated data to file "{}"'.format(data_filepath))
            with open(data_filepath, 'wb') as wb:
                wb.write(data)
        megabytes = os.path.getsize(data_filepath) / MEGABYTE
        megabyte_str = '{:0.3f} mb'.format(megabytes)
        print('data is os.path.getsize', megabyte_str)
    except (KeyboardInterrupt, Exception):
        print('could not create file, dying.')
        raise

    print('starting benchmark...')
    i = 0
    start = datetime.datetime.now()
    delta = datetime.timedelta(seconds=args.time)
    samefile = os.path.join(dirpath, str(uuid.uuid4()))
    loop = True
    while loop:
        i += 1
        try:
            if not args.overwrite:
                filepath = os.path.join(dirpath, str(i))
            print(filepath, ' writing...', end=' ')
            write = datetime.datetime.now()
            # res = shutil.copy(data_filepath, filepath)  # this causes disc writes on the source disk
            with open(filepath, 'wb') as wb:
                wb.write(data)
            write_time = datetime.datetime.now() - write
            writes.append(write_time)
            write_rate = megabytes / write_time.total_seconds()
            print('{:0.3f} mb/s'.format(write_rate), end=' ')

            print('reading...', end=' ')
            read = datetime.datetime.now()
            # BUG: this is wrong... whatever i'm reading isnt right
            #   on my sata3 ssd its 1792.215 mb/s, thats impossible
            with open(filepath, 'rb') as rb:
                written = rb.read()  # its as if this read is completely skipped...
            read_time = datetime.datetime.now() - read
            reads.append(read_time)
            read_rate = megabytes / read_time.total_seconds()
            print('{:0.3f} mb/s'.format(read_rate), end=' ')

            if data != written:
                print('data integrity failed on iteration', i)
                break
            print()

        except KeyboardInterrupt:
            print()
            print('cancelling...')
            break
        except ZeroDivisionError:
            print()
            print('turns out, the writes/reads are so fast that python thinks it took 0 seconds... increase size?')
            raise
        # TODO: OSError: [Errno 28] No space left on device

        if args.time != -1:
            loop = datetime.datetime.now() - start < delta
            if not loop:
                print('time exceeded', args.time, 'seconds')

    print('done!')
    elapsed = datetime.datetime.now() - start
    write_time = sum(w.total_seconds() for w in writes) / i
    write_rate = megabytes / write_time
    read_time = sum(r.total_seconds() for r in reads) / i
    read_rate = megabytes / read_time
    print('elapsed   :', elapsed, 's')
    print('write rate:', '{:0.3f} mb/s'.format(write_rate))
    print('read rate :', '{:0.3f} mb/s'.format(read_rate))
