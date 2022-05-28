package main

import (
	"fmt"
	"log"
	"os"

	"github.com/abesheknarayan/go-caskdb/stores"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("failed to load env file")
	}
	path := os.Getenv("DB_PATH")
	booksDb, err := stores.InitDb("test", path)
	if err != nil {
		fmt.Println(err)
	}
	booksDb.Set("name", "abeshek")
	fmt.Println(booksDb.Get("name"))
	booksDb.Set("movie", "top gun maverick")
	fmt.Println(booksDb.Get("movie"))
	booksDb.CloseDB()
	// booksDb.Cleanup()
}
