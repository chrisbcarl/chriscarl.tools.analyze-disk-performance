# analyze-disk-performance
basically I hate using FIO, CrystalDiskMark, etc, lemme write something quick that does everything I want and I understand it all.
for now it behaves like a sequential write, doesn't have any other bells or whistles


# Usage
```bash
python main.py /tmp --mb 10 --time 60
python main.py \\unc --mb 10 --time 60
```

# Setup
- offline (download materials on donor, and transfer to daughter):
    1. setup environment on donor
    2. download python installer
    3. run these commands on donor
        ```bash
        mkdir packages
        python -m pip freeze > requirements.txt
        ```
    4. transfer files to daughter
    5. run these commands on daughter
        ```bash
        pip install -r requirements.txt --no-index --find-links <packages>
        ```


# Arguments
* `--time`: if not provided, loops infinitely, otherwise terminates if over `--time` seconds
* `--mb`: size in megabytes (1024 bytes) to read/write
* `--overwrite`: replace files repeatedly rather than fill up the drive
    * ex) omitting `--time` and `--overwrite` **will** cause the disk to fill and the program to crash and maybe the operating system to crash. your call.


# Improvements
## Features
- `--mb` should be a list so people can iterate through 1kb, 10mb, 100mb, etc.
- `path` should be a list so people can iterate through drives if they like.
- better reporting or at least table-like or something
- progress bar would be nice
- delete all files after would be nice
- export to log file like fio does, bandwidth, etc.

## Bugs

## Performance
- the array implementation is causing memory to spike unusustainably
- very small files cause imperceptible time delta, somehow make it more precise?
- the megabytes aren't really correctly sized... might want to go with the buffer fill approach rather than the array fill hope and pray approach
