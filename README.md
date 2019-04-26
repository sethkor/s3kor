# s3kor
S3 tools built in GoLang using threads for fast parallel actions like copy, list and remove to S3.

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
|Linux         | amd64, arm64, arm6, arm7, 386 (32 bit) |
|Windows       | amd64, 386 (32 bit)                    |

Let me know if you would like a particular os/arch binary regularly built.

Or build it yourself by go getting it:
```
go get github.com/sethkor/s3kor
```


The cli emulates the [aws cli](https://aws.amazon.com/cli/) as close as possible so as to be a drop in replacement.  Supported s3 operations:
-[X] List - ls
-[X] Remove - rm
-[ ] Copy - cp
-[ ] Syncronize - sync

Use `--help` on the command line to help you along the way.

```cassandraql
usage: s3kor [<flags>] <command> [<args> ...]

s3 tools using golang concurency

Flags:
  --help             Show context-sensitive help (also try --help-long and --help-man).
  --profile=PROFILE  AWS credentials/config file profile to use
  --auto-region      Automatic region detection
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

```

--profile will always look in the usual place for your aws `credentials` or `config` file

###Automatic region detection for your buckets
All commands can take the `--auto-region` flag to automatically detect the right region for your bucket operation, rather than you passing the specific region with `--region`.


##List - ls
Nothing special here.  Just remember S3 has prefixes, not directory paths.

##Remove - rm
```cassandraql
  --recursive        Recurisvley delete
  --all-versions     Delete all versions
```

Both options can be used together in a single command.

Remember when using `--all-versions` to delete all versions of an object at once, the eventual consistency model of S3 applies.

When deleting a large number of objects, the final outcome may not be reflected by `ls` immediately due to eventual consistency.

##Copy - cp
This is WIP

##Sync - cp
This is WIP





