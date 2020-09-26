#!/usr/bin/python3

import sys
import time
import boto3
import queue
import logging
import argparse
import datetime
import threading
import multiprocessing
from os import path
from botocore.exceptions import ClientError

AWS_PROFILE = 'default'
PERCENT_QUEUE = queue.Queue()


def setup_logger(logfile):
    logger = logging.getLogger(f's3_restore_{logfile}')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    logger.addHandler(fh)

    return logger


def read_file(fname):
    '''read file per line to array'''
    lines = []
    with open(fname) as f:
        for i, l in enumerate(f):
            lines.append(l.replace('\n', ''))

    return lines


def chunks(lst, n):
    '''generator, yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def diff(first, second):
    '''diff between two arrays'''
    second = set(second)
    return [item for item in first if item not in second]


def refresh_credentials(thread_id=0):
    session = boto3.session.Session(profile_name=AWS_PROFILE)
    s3 = session.client('s3')
    return s3


def request_retrieval(progress_logger, availability_logger, files, bucket_name, retain_days, tier, chunk_index):
    '''
    reqest object retrieval from supplied 'files' array
    'files' array should contain s3 paths eg. 2018/06/10/file.txt
    '''

    s3_client = refresh_credentials(chunk_index)
    counter = 0
    for f in files:
        try:
            response = s3_client.restore_object(
                Bucket=bucket_name,
                Key=f,
                RestoreRequest={
                    'Days': int(retain_days),
                    'GlacierJobParameters': {
                        'Tier': tier
                    }
                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f'{f} already available to download')
                availability_logger.info(f)
            elif response['ResponseMetadata']['HTTPStatusCode'] == 202:
                progress_logger.info(f)

        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'NoSuchKey':
                print(f'{f} not found, skipping')
            elif code == 'RestoreAlreadyInProgress':
                print(f'{f} restore already in progress, ignoring')
                progress_logger.info(f)
            elif code == 'ExpiredToken':
                s3_client = refresh_credentials(chunk_index)
            else:
                print(f'{f}: {e}')

        counter += 1
        actual_percent = counter / len(files)
        PERCENT_QUEUE.put([chunk_index, actual_percent])


def check_files_availability(availability_logger, files, bucket_name, chunk_index):
    '''does a HEAD request on files array to check if s3 object restore is already complete or is still in progress'''
    s3_client = refresh_credentials(chunk_index)

    counter = 0
    for f in files:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=f)
            if 'x-amz-restore' in response['ResponseMetadata']['HTTPHeaders']:
                x_amz_restore = response['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
                if 'ongoing-request="false"' in x_amz_restore:  # false = restore complete, true = restore still in progress
                    availability_logger.info(f)

        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'NoSuchKey':
                print(f'{f} not found, skipping')
            elif code == 'ExpiredToken':
                s3_client = refresh_credentials(chunk_index)
            else:
                print(f'Exception occured: {e}')

        counter += 1
        actual_percent = counter / len(files)
        PERCENT_QUEUE.put([chunk_index, actual_percent])


def print_percent_queue(percent_dict):
    while PERCENT_QUEUE.empty() is False:
        data = PERCENT_QUEUE.get(timeout=0.1)
        percent_dict[data[0]] = data[1]

    out_str = ''
    total_percent = 0
    for chunk_id, percent in percent_dict.items():
        percent *= 100
        total_percent += percent
        out_str += f' T{chunk_id}: {percent}% '

    if(len(percent_dict) > 0):
        total_percent /= len(percent_dict)
        out_str = f'Total: {total_percent:.2f}% [{out_str}]'
        print(out_str)


def main_generate_list(bucket, output_filename):
    '''generates a file list from whole bucket (only files in glacier or deep_archive tier)'''

    if path.exists(output_filename):
        input_overwrite_continue = input(f'File {output_filename} already exists and will be overwritten\nContinue? y/[n]: ')
        if input_overwrite_continue != 'y':
            return

    s3_client = refresh_credentials()

    glacier_objects = []
    print('Listing objects to file')
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)
        last_count = 0
        for page in pages:
            for obj in page['Contents']:
                if obj['StorageClass'] == 'GLACIER' or obj['StorageClass'] == 'DEEP_ARCHIVE':
                    glacier_objects.append(obj['Key'])
                    if len(glacier_objects) >= last_count+1000:
                        last_count = len(glacier_objects)
                        print(f'Found: {last_count}')

    except ClientError as e:
        print(e)

    print(f'Total count: {len(glacier_objects)} glacier/deep_archive objects saved to {output_filename}')

    with open(output_filename, 'w') as output_list:
        for obj in glacier_objects:
            output_list.write(f'{obj}\n')


def main_request_objects_restore(bucket, object_list_path, retain_for, retrieval_tier, thread_count):

    progress_logfile = f'{bucket}.progress'
    availability_logfile = f'{bucket}.available'
    progress_logger = setup_logger(progress_logfile)
    availability_logger = setup_logger(availability_logfile)

    progress_log = []
    if path.exists(progress_logfile):
        progress_log = read_file(progress_logfile)
    availability_log = []
    if path.exists(availability_logfile):
        availability_log = read_file(availability_logfile)

    print('')
    lines = read_file(object_list_path)
    if len(progress_log) > 0:
        prev_len = len(lines)
        lines = diff(lines, progress_log)
        print(f'Progress log found. Skipping {prev_len - len(lines)} entries')

    if len(availability_log) > 0:
        prev_len = len(lines)
        lines = diff(lines, availability_log)
        print(f'Availability log found. Skipping {prev_len - len(lines)} entries (restore is complete on these files)')

    if len(lines) == 0:
        print('All objects already requested, nothing to do')
        sys.exit(1)

    print(f'Will have to process {len(lines)} files')

    if len(lines) < int(thread_count):
        thread_count = len(lines)

    split_by = max(int(len(lines) / int(thread_count)), 1)

    est_hours = len(lines)/int(thread_count)/5/60/60  # 5 -> single thread can request approx 5 objects/s
    est_hours_format = str(datetime.timedelta(hours=est_hours)).split('.')[0]
    print(f'{thread_count} threads, {split_by} files per thread')
    if input(f'This will take approximately { est_hours_format }\nContinue? (y/[n]): ') != 'y':
        sys.exit(1)

    threads = []
    timer_start = time.time()
    chunk_index = 0
    for chunk in chunks(lines, split_by):
        t = threading.Thread(target=request_retrieval, args=(progress_logger, availability_logger, chunk, bucket, retain_for, retrieval_tier, chunk_index), daemon=True)
        t.start()
        threads.append(t)
        chunk_index += 1

    percent_dict = {}
    while any(thread.is_alive() for thread in threads):
        print_percent_queue(percent_dict)
        time.sleep(1)
    
    print_percent_queue(percent_dict)

    exec_time = str((time.time()-timer_start)).split('.')[0]
    print(f'Execution took {exec_time}s')


def main_check_restore_status(bucket, object_list_path, thread_count):

    availability_logfile = f'bucket_{bucket}.available'
    availability_logger = setup_logger(availability_logfile)

    availability_log = []
    file_list = []

    if not path.exists(object_list_path):
        print(f'{object_list_path} not found. Cancelling')
        print('If you dont have any file with path list, run `Generate file list` option first')
        return

    print('')
    file_list = read_file(object_list_path)

    if path.exists(availability_logfile):
        availability_log = read_file(availability_logfile)

    if len(availability_log) > 0:
        prev_len = len(file_list)
        file_list = diff(file_list, availability_log)
        print(f'Availability log found. Skipping {prev_len - len(file_list)} entries (these files are ready for download)')

    print(f'Will have to process {len(file_list)} files')

    split_by = max(int(len(file_list) / int(thread_count)), 1)

    est_hours = len(file_list)/int(thread_count)/14/60/60  # 5 -> single thread can request approx 14 objects/s
    est_hours_format = str(datetime.timedelta(hours=est_hours)).split('.')[0]
    print(f'{thread_count} threads, {split_by} files per thread')
    if input(f'This will take approximately { est_hours_format }\nContinue? (y/[n]): ') != 'y':
        sys.exit(1)

    threads = []
    timer_start = time.time()
    chunk_index = 0
    for chunk in chunks(file_list, split_by):
        t = threading.Thread(target=check_files_availability, args=(availability_logger, chunk, bucket, chunk_index), daemon=True)
        t.start()
        threads.append(t)
        chunk_index += 1

    percent_dict = {}
    while any(thread.is_alive() for thread in threads):
        print_percent_queue(percent_dict)
        time.sleep(0.1)

    print_percent_queue(percent_dict)

    print(f'Execution took {time.time()-timer_start}')
    print('')
    new_availability_list = read_file(availability_logfile)
    new_file_list = read_file(object_list_path)
    print(f'{len(new_availability_list)} files are restored and ready for download')
    print(f'{len(new_file_list)-len(new_availability_list)} files is still being restored')


def main():
    global AWS_PROFILE

    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--aws-profile', default='default')

    subparsers = parser.add_subparsers(dest='subcommand', required=True)

    generate_parser = subparsers.add_parser('generate-object-list')
    generate_parser.add_argument('--output-object-list-path')

    request_parser = subparsers.add_parser('request-objects-restore')
    request_parser.add_argument('--retain-for', required=True, help='How long to keep objects restored')
    request_parser.add_argument('--object-list-path')
    request_parser.add_argument('--retrieval-tier', default='Standard', choices=['Standard', 'Bulk', 'Expedited'])
    request_parser.add_argument('--thread-count', default=int(multiprocessing.cpu_count()))

    check_parser = subparsers.add_parser('check-objects-status')
    check_parser.add_argument('--object-list-path')
    check_parser.add_argument('--thread-count', default=int(multiprocessing.cpu_count()))

    args = parser.parse_args()

    AWS_PROFILE = args.aws_profile

    if args.subcommand == 'generate-object-list':
        print('Command: Generate list of objects to restore from specified S3 bucket')
        
        if args.output_object_list_path is None:
            args.output_object_list_path = f'{args.bucket}.objects'

        main_generate_list(args.bucket, args.output_object_list_path)

    elif args.subcommand == 'request-objects-restore':
        print('Command: Request restoration of objects')

        if args.object_list_path is None:
            args.object_list_path = f'{args.bucket}.objects'

        main_request_objects_restore(args.bucket, args.object_list_path, args.retain_for, args.retrieval_tier, args.thread_count)

    elif args.subcommand == 'check-objects-status':
        print('Command: Check objects status to verify completeness')

        if args.object_list_path is None:
            args.object_list_path = f'{args.bucket}.objects'

        main_check_restore_status(args.bucket, args.object_list_path, args.thread_count)


if __name__ == '__main__':
    main()
