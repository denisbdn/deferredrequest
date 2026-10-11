all: compile test

compile:
	go get && go build

.PHONY: test
test:
	go test -v ./local/callbacks
