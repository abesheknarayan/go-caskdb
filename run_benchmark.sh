echo "... Running Benchmarks ..."
export CASKDB_ENV=Test
go test -race -v -bench=. -run="^$" ./... > logs/benchmark/blog.log
# go test -race -v -bench=. -run="^$" ./...
