# dead code stuff
```python
def fill_disk(output_dirpath=OUTPUT_DIRPATH):
    import math
    import sys
    from concurrent.futures import ThreadPoolExecutor
    import shutil
    import uuid
    if not os.path.isdir(output_dirpath):
        os.makedirs(output_dirpath)
    disk_free_bytes = psutil.disk_usage('/').free
    power = math.ceil(math.log(disk_free_bytes, 1024))  # 1, 2, 3, 4 (at 64gb remaining, power was 3.65)
    logging.info('generating files to write...')
    mb = (1024**2)  # 1mb
    large_file_size = 10**(power - 1)
    byte_str = create_bytearray(mb)
    source_file = os.path.join(output_dirpath, 'donor')
    logging.info('using files of size %0.3f MB to write...', mb * large_file_size)
    with open(source_file, 'wb') as wb:
        for i in range(large_file_size):  # 100mb
            if i % 10**(power - 1) == 0:
                logging.info('large file written: %0.3f%%', i / large_file_size)
            wb.write(byte_str)
    disk_free_bytes = psutil.disk_usage('/').free
    files_to_write = [os.path.join(output_dirpath, str(uuid.uuid4())) for _ in range(disk_free_bytes // (mb * large_file_size))]
    event = threading.Event()
    t = threading.Thread(target=disk_usage_monitor, args=(event, ), daemon=True)
    t.start()
    logging.info('writing %s files', len(files_to_write))
    try:
        with ThreadPoolExecutor(max_workers=CPU_COUNT - 2) as executor:
            for dest_filepath in files_to_write:
                executor.submit(shutil.copy, source_file, dest_filepath)
    finally:
        event.set()
    du = psutil.disk_usage('/')
    logging.info('disk usage: %s%%', du.percent)
```