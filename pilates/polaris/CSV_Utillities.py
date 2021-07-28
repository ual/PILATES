#!/usr/bin/python
# Filename: CSV_Utilities.py

# import shutil
# import sys
# import os
# import subprocess
# from shutil import copyfile
from pathlib import Path
# import json
# import sqlite3
import csv
# import traceback
import queue


def append_column(src, tgt, loop, column, header_text):
    all_data = []
    # new_data = []

    with src.open('r') as csv_input:
        reader = csv.reader(csv_input)

        if loop == 0:
            next(reader)  # read header row
            output_row = []
            if loop == 0:
                output_row.append('time')
            output_row.append(header_text)
            all_data.append(output_row)

            for input_row in reader:
                output_row = []
                if loop == 0:
                    output_row.append(input_row[0])
                output_row.append(input_row[column])
                all_data.append(output_row)

            with tgt.open('w') as csv_output:
                writer = csv.writer(csv_output, lineterminator='\n')
                writer.writerows(all_data)
        else:
            new_data_queue = queue.Queue()
            next(reader)  # read header (and ignore)
            for input_row in reader:
                new_data_queue.put(input_row[column])

            with tgt.open('r') as csv_existing:
                existing = csv.reader(csv_existing)
                header = next(existing)  # read header row
                output_row = []
                for h in header:
                    output_row.append(h)
                output_row.append(header_text)
                all_data.append(output_row)
                for existing_row in existing:
                    output_row = []
                    for e in existing_row:
                        output_row.append(e)
                    output_row.append(new_data_queue.get())
                    all_data.append(output_row)

            with tgt.open('w') as csv_output:
                writer = csv.writer(csv_output, lineterminator='\n')
                writer.writerows(all_data)


# if __name__ == '__main__':
#     # if len(sys.argv) < 3:
#     #     print('Usage %s <source_file> <target_file>' % (sys.argv[0]))
#     #     sys.exit(-1)
#     #
#     # src_file = Path(sys.argv[1])
#     # if not src_file.exists():
#     #     print('ERROR: Source File %s does not exist!' % src_file)
#     #     sys.exit(-1)
#
#     # tgt_file = Path(sys.argv[2])
#     # if not tgt_file.exists():
#     #     print('ERROR: Target File %s does not exist!' % tgt_file)
#     #     sys.exit(-1)
#
#     src_files = [Path('D:/rweimer/polaris/Grid/grid_abm42/summary.csv'),
#                  Path('D:/rweimer/polaris/Grid/grid_abm43/summary.csv')]
#     tgt_file = Path('D:/rweimer/polaris/Grid/test.csv')
#     folders = src_files[0].parts
#     append_column(src_files[0], tgt_file, 0, 4, str(folders[len(folders)-2]))
#     folders = src_files[1].parts
#     append_column(Path(src_files[1]), tgt_file, 1, 4, str(folders[len(folders)-2]))
