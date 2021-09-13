all: linux windows

mac:
	env GOOS=darwin GOARCH=amd64 go build -o s3kor

linux:
	env GOOS=linux GOARCH=amd64 go build -o s3kor.linux
	gzip -fk9 s3kor.linux

windows:
	env GOOS=windows GOARCH=amd64 go build -o s3kor.exe
	gzip -fk9 s3kor.exe

clean:
	rm -f s3kor.linux s3kor.linux.gz s3kor.exe s3kor.exe.gz s3kor

publish-test:
	goreleaser --snapshot --rm-dist

publish:
	goreleaser --rm-dist --skip-validate
