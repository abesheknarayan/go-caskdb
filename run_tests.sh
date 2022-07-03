echo "...running unit tests..."
export CASKDB_ENV=Test
mkdir logs/test/
go test -race -run="^(Test|Benchmark)[^_](.*)" ./... > logs/test/unit.log
code=$?

if [ $code -ne 0 ]; then
    exit $code
fi

printf "\nUnit Tests complete. Performing Integration Tests now.\n"

echo "...running integration tests..."

# go test -v -race -run="^(Test|Benchmark)_(.*)" ./... > logs/test/integration.log
go test -v -race -run="^(Test|Benchmark)_(.*)" ./... > logs/test/integration.log

code=$?
echo $code
exit $code

