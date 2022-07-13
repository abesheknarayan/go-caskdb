echo "... Running Benchmarks ..."
export CASKDB_ENV=Test
go test -race -v -bench=. -run="^$" -count=5 ./... > benchmark/benchmark.txt
benchstat benchmark/benchmark.txt > benchmark/stat.txt
# go test -race -v -bench=. -run="^$" ./...
