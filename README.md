# AWS S3 Glacier Restore tool

I've created this tool in purpose of restoring huge number of objects stored in Glacier tier in AWS S3 bucket  

## Problem
AWS have tier which is called Glacier, it costs a fraction of S3 Standard tier and is primarily used for storing long term backups but it have it's specifics

1) Restore takes up to 48 hours for files to be available for download
2) You need to request retrieval for every single object (file) in bucket
3) You don't know which files are already ready for download and you have to check them all again and againt

## Solution
This tool  

1) It can generate a list of all objects in bucket if you need to restore whole bucket, or, you can supply your own list of object which you need to retrieve
2) It saves a progress where it left off and list of files it already requested for retrieval so it save you time if you need to check multiple times
3) Is multithreaded so it can request multiple files at once! (this was a huge boost, from days to hours!)
4) Allows you to simply check status of requested files and keep list of files which are already ready for download and does not check them again

## Usage
You always need to specify `--bucket` parameter (name of bucket)  
You can specify an `--aws-profile` parameter to use specified profile from `~/.aws/config`

### Subcommands
#
`generate-object-list` ganerates a file list into `<bucket>.objects` file  
#
`request-objects-restore` uses `<bucket>.objects` object list and saves names of already requested objects to `<bucket>.progress` file

Parameteres:  
`--retain-for` number of days you want to keep objects restored [Required]  
`--retrieval-tier` Standard, Bulk, Expedited (default: Standard) [Optional]  
`--thread-count` (default: num of cpu in your machine) [Optional]
#
`check-objects-status` uses `<bucket>.objects` object list, compares it to `<bucket>.available` and check only files which are not already ready for download  

Parameters:
`--thread-count` (default: num of cpu in your machine) [Optional]
#

### Generating a list
```
./s3_restore.py --bucket <your_bucket> genereate-object-list
```
If you have your own list of files you want to restore, save it to file and name it `<your_bucket>.objects`

### Requesting a retrieval
```
./s3_restore.py --bucket <your_bucket> request-objects-restore --retain-for 10
```

### Checking objects retrieval status
```
./s3_restore.py --bucket <your_bucket> check-objects-status
```

## Benchmarks
I did not create any benchmarks but in my case requesting retrieval of 700 000 files took around 6-8 hours (doing it naive way would took several days!)

## Performance 
For best performance i recommend creating an EC2 Instance (preferably in same region as your bucket, and with many cores to utilize multithreading) and running it from there, latency is so much lower and in benefit of it the request rate is so much higher :)

The AWS S3 `POST` request limit is around 3500req/s so be aware that there are paralelization limits and set your thread count accordingly (if i remember correctly the request rate for single thread is around 5-10req/s)

## Remarks
This tool is the fastest one available on the internet (or at least was in times of writing [mid 2019])  
**If you found something better let me know!**