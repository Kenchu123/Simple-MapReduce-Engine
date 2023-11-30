all: build

build:
	mkdir -p bin
	go build -o bin/sdfs main.go
	go build -o maple_wordcount  exe/maple_wordcount/maple_wordcount.go
	go build -o juice_wordcount exe/juice_wordcount/juice_wordcount.go

run:
	go run main.go $(ARGS)

clean:
	rm -rf bin
	rm -rf logs/*/*.log
	rm -rf blocks/*/*
