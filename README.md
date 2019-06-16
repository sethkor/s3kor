# s3kor
Fast AWS S3 command line tools built in [Go](https://golang.org/) using multiparts and multiple threads for fast parallel actions like copy, list and remove to AWS S3.  It's intended as a drop in replacement for the [aws cli s3](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html) set of commands so all flags, values and args should be the same with the exception of a few new ones.

Easiest way to install if you're on a Mac or Linux (amd64 or arm64)  is to use [Homebrew](https://brew.sh/)

Type:

```
brew tap sethkor/tap
brew install s3kor
```

For other platforms take a look at the releases in Github.  I build binaries for:

|OS            | Architecture                           |
|:------------ |:-------------------------------------- |
|Mac (Darwin)  | amd64 (aka x86_64)                     |
|Linux         | amd64, arm64, 386 (32 bit) |
|Windows       | amd64                   |

Let me know if you would like a particular os/arch binary regularly built.

Or build it yourself by go getting it:
```
go get github.com/sethkor/s3kor
```


The cli emulates the [aws cli s3](https://aws.amazon.com/cli/) commands as close as possible so as to be a drop in replacement.  Supported s3 operations:

- [X] Copy - cp
- [X] Remove - rm
- [X] List - ls
- [ ] Syncronize - sync

Use `--help` on the command line to help you along the way.

```cassandraql
usage: s3kor [<flags>] <command> [<args> ...]

s3 tools using golang concurency

Flags:
  --help             Show context-sensitive help (also try --help-long and --help-man).
  --profile=PROFILE  AWS credentials/config file profile to use
  --region=REGION    AWS region
  --verbose          Verbose Logging
  --version          Show application version.

Commands:
  help [<command>...]
    Show help.

  rm [<flags>] <S3Uri>
    remove

  ls [<flags>] <S3Uri>
    list

  cp [<flags>] <source> <destination>
    copy
```

`--profile` will always look in the usual place for your aws `credentials` or `config` file.

### Automatic region detection for your buckets
All commands can take the `--auto-region` flag to automatically detect the right region for your bucket operation, rather than you passing the specific region with `--region`.

## Copy - cp
This is WIP, some further features to come.  Features available:

- [X] Copy to S3
- [X] Copy from S3
- [X] Copy S3 to S3
- [X] Copy S3 to S3 in another account with seperate credentials

Please raise an issue if theres a specific feature you. would like considered or prioritised.

```
  -q, --quiet                   Does not display the operations performed from the specified command.
  -r, --recursive               Recursively copy
  -c, --concurrent=50           Maximum number of concurrent uploads to S3.
      --sse=AES256              Specifies server-side encryption of the object in S3. Valid values are AES256 and aws:kms.
      --sse-kms-key-id=SSE-KMS-KEY-ID  
                                The AWS KMS key ID that should be used to server-side encrypt the object in S3.
      --acl=private             Object ACL
      --storage-class=STANDARD  Storage Class
      --dest-profile=DEST-PROFILE  
                                Destination bucket AWS credentials/config file profile to use if different from --profile

Args:
  <source>       file or s3 location
  <destination>  file or s3 location
```

The maximum concurrent uploads (`--concurrent` or `-c`) is dependent not only on your upload bandwidth but also the maximum open file limits per process on your system and the performance of the soucre drive.  

You can check your file limits in linux, macos and other flavour of OS with `ulimit -n`.  Changing this limit in the os is possible and not always dangerous.  Instructions on how to change it vary between OS so they are not described here.  `s3kor` impacts these limits both in walking the file system and uploading the file so there is not a 1 to 1 correlation between the max limit ond the value you pass to `--concurrent`.  Try to pass `s3kor` a max value that is about 20% less than the systems max limit value.

Currently if you hit a file limit, the error is not reported.

For optimal throughput consider using a S3 VPC Gateway endpoint if you are executing s3kor from within an AWS VPC.

And remember the performance of the source storage device is important, you don't want to choke it reading lots of data at once.  Use an optimized iops device or SAN.

### Supports different AWS account credentials for source and destination buckets

If you ever need to copy objects to an account which you don't own and need a seperate set of AWS credentials to access it, s3kor is perfect for the job.  For S3 to S3 with different AWS credentials, objects must be downloaded from the source first and then uploaded.  To cater for large objects and to limit memory usage this is done utilizing multi parts of 5MB in size and will attempt to limit in memory storage.  This relies on GO garbage collector to tidy things up promptly which doesn't always happen.  Some further optimizations are WIP but expect this operation to consume a bit of memory.

### Multiparts
Multipart chunks size for Upload or Download is set to 5MB.  Any objects greater than 5MB in size shall be sent in multiparts. `s3kor` will send up to 5 parts concurrently per object.

### Progress Bar Behaviour
The progress bar is not aware of multipart operations for now.  It will only update once a large object operation is complete.

### S3 to S3 Large objects over 5GB
Large objects S3 to S3 are supported.  Since this happens in the AWS backend, the part size for these operations is set to 5GB for maximum performance.  `s3kor` will copy up to 5 parts concurrently for per object.

### ACL
Sets the ACL for the object when the command is performed. If you use this parameter you must have the "s3:PutObjectAcl" permission included in the list of actions for your IAM policy.  Only accepts values of `private`, `public-read`, `public-read-write`, `authenticated-read`, `aws-exec-read`, `bucket-owner-read`, `bucket-owner-full-control` and `log-delivery-write`

Defaults to `private`

### Storage Class
The type of storage to use for the object. Valid choices are: `STANDARD`, `EDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`. 

Defaults to `STANDARD`

## Remove - rm
```
  -q, --quiet            Does not display the operations performed from the specified command.
  -r, --recursive        Recurisvley delete
      --all-versions     Delete all versions
```

It is possible to remove all versions of an object recursivley in a single command which is not possible with the AWS cli

Remember when using `--all-versions` to delete all versions of an object at once, the eventual consistency model of S3 applies.

When deleting a large number of objects, the final outcome may not be reflected by `ls` immediately due to eventual consistency.

## List - ls
Nothing special here.  Just remember S3 has prefixes, not directory paths.

You can list all versions if you like, the version id is outputted first:

```
      --all-versions     List all versions
```
  
## Sync - sync
This is WIP





