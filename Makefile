mac:
	env GOOS=darwin GOARCH=amd64 go build -o s3kor

linux:
	env GOOS=linux GOARCH=amd64 go build -o s3kor.linux
	gzip -fk9 s3kor.linux

windows:
	env GOOS=windows GOARCH=amd64 go build -o s3kor.windows
	gzip -fk9 s3kor.windows

clean:
	rm s3kor.linux s3kor.linux.gz s3kor.windows s3kor.windows.gz s3kor

publish-test:
	goreleaser --snapshot --rm-dist

publish:
	goreleaser --rm-dist --skip-validate
