all: build

build:
	mkdir -p bin
	go build -o bin/sdfs main.go
	go build -o maple_wordcount  exe/maple_wordcount/maple_wordcount.go
	go build -o juice_wordcount exe/juice_wordcount/juice_wordcount.go
	go build -o maple_demo exe/maple_demo/maple_demo.go
	go build -o juice_demo exe/juice_demo/juice_demo.go
	go build -o maple_filter exe/maple_filter/maple_filter.go
	go build -o juice_filter exe/juice_filter/juice_filter.go
	go build -o maple_join exe/maple_join/maple_join.go
	go build -o juice_join exe/juice_join/juice_join.go
	go build -o filter sql/filter/filter.go
	go build -o join sql/join/join.go
	cp sql/jar/*.jar .

run:
	go run main.go $(ARGS)

clean:
	rm -rf bin
	rm maple_wordcount
	rm juice_wordcount
	rm maple_demo
	rm juice_demo
	rm maple_filter
	rm juice_filter
	rm maple_join
	rm juice_join
	rm filter
	rm join
	rm -rf logs/*/*.log
	rm -rf blocks/*/*
	rm *.jar
