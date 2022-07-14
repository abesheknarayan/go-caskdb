echo "...running unit tests..."
export CASKDB_ENV=Test
mkdir logs/
mkdir logs/test/


go clean -testcache
# go test -race -run="^(Test|Benchmark)[^_](.*)" ./... > logs/test/unit.log
go test -race -run="^(Test)[^_](.*)" ./...

code=$?

if [ $code -ne 0 ]; then
    exit $code
fi

printf "\nUnit Tests complete. Performing Integration Tests now.\n"

echo "...running integration tests..."

# go test -v -race -run="^(Test|Benchmark)_(.*)" ./... > logs/test/integration.log
go test -v -race -run="^(Test)_(.*)" ./...  > logs/integration.log
# go test -v -race -run="^(Test)_(.*)" ./... 


code=$?
echo $code
exit $code

