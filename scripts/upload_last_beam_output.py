import argparse
import sys
import os
import s3fs

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Upload Beam last run on s3')

    parser.add_argument(
        '-b', '--bucket', action='store', dest='s3bucket',
        help='S3 bucket name',
        required=True)
    parser.add_argument(
        '-o', '--output-data-dir', action='store', dest='output_data_dir',
        help='path to beam output dir',
        required=True)
    parser.add_argument(
        '-s', '--s3-data-dir', action='store', dest='s3_data_dir',
        help='path of s3 dir',
        required=True)

    options = parser.parse_args()

    s3bucket = options.s3bucket
    output_data_dir = options.output_data_dir
    s3_data_dir = options.s3_data_dir

    s3 = s3fs.S3FileSystem(anon=False)
    main_root = os.path.join(os.getcwd(),output_data_dir)
    for root, subdirs, files in os.walk(main_root):

        for filename in files:
            file_path = os.path.join(root, filename)
            rel_path = os.path.relpath(file_path, main_root)
            s3.put(file_path, s3bucket + "/" +s3_data_dir+"/"+rel_path)