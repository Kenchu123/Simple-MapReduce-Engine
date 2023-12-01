all: build

build:
	mkdir -p bin
	go build -o bin/sdfs main.go
	go build -o maple_wordcount  exe/maple_wordcount/maple_wordcount.go
	go build -o juice_wordcount exe/juice_wordcount/juice_wordcount.go
	go build -o maple_demo exe/maple_demo/maple_demo.go
	go build -o juice_demo exe/juice_demo/juice_demo.go
	go build -o filter sql/filter/filter.go
	go build -o join sql/join/join.go

run:
	go run main.go $(ARGS)

clean:
	rm -rf bin
	rm maple_wordcount
	rm juice_wordcount
	rm maple_demo
	rm juice_demo
	rm filter
	rm join
	rm -rf logs/*/*.log
	rm -rf blocks/*/*
