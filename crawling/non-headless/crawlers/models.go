package crawlers

import (
	"database/sql"
	"log"
	"time"
)

type Crawler interface {
	Run(*sql.DB)
}

type Article struct {
	title   string
	date    time.Time
	content string
	url     string
	hashId  string
	ticker  string
}

func CheckHashIdExists(hashId string, db *sql.DB) bool {
	var articleExists bool
	stmt := `SELECT EXISTS (SELECT 1 FROM noticias WHERE hash_id = $1)`
	err := db.QueryRow(stmt, hashId).Scan(&articleExists)
	if err != nil {
		log.Fatal(err)
	}

	return articleExists
}

func SaveArticle(a *Article, db *sql.DB) bool {
	if CheckHashIdExists(a.hashId, db) {
		return true
	}

	stmt := `INSERT INTO noticias(title, content, date, url, hash_id) VALUES ($1, $2, $3, $4, $5)`
	_, err := db.Exec(stmt, a.title, a.content, a.date, a.url, a.hashId)
	if err != nil {
		log.Fatal(err)
	}
	return true
}
