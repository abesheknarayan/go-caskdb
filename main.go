package main

import (
	"fmt"

	"github.com/abesheknarayan/go-caskdb/stores"
)

func main() {
	booksDb, err := stores.InitDb("books")
	if err != nil {
		fmt.Println(err)
	}
	booksDb.Set("name", "abeshek")
	fmt.Println(booksDb.Get("name"))
	booksDb.Set("movie", "top gun")
	fmt.Println(booksDb.Get("movie"))

	booksDb.CloseDB()
}
