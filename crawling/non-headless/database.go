package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func OpenConn() (*sql.DB, error) {
	connStr := "host=localhost port=5436 user=postgres password=postgres dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	return db, err
}
