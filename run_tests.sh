echo "...running unit tests..."
export CASKDB_ENV=Test
go test -race -run="^(Test|Benchmark)[^_](.*)" ./... 
code=$?

if [ $code -ne 0 ]; then
    exit $code
fi

printf "\nUnit Tests complete. Performing Integration Tests now.\n"

echo "...running integration tests..."

go test -v -race -run="^(Test|Benchmark)_(.*)" ./...
code=$?

exit $code

