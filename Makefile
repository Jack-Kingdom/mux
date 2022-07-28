test:
	go test -v -count=1

bench:
	go test -v -bench .

dep:
	go mod tidy