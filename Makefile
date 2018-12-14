all:
	env GOOS=linux GOARCH=amd64 go build -o s3kor.linux
	gzip -fk9 s3kor.linux

mac:
	env GOOS=darwin GOARCH=amd64 go build -o s3kor.mac
	
clean:
	rm s3kor.linux s3kor.linux.gz
