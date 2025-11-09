package main

import (
	"crawler/crawlers"
	"fmt"
	"log"
)

var mapTickerFeed = map[string]string{
	"vale3": "https://br.investing.com/equities/vale-on-n1-news",
	"csna3": "https://br.investing.com/equities/sid-nacional-on-news",
}

func getCrawlers(searchTerm string, ticker string) (*crawlers.Investing, *crawlers.Bloomberg, *crawlers.InvestNews) {
	queryUrl := mapTickerFeed[ticker]
	i := crawlers.NewInvesting(queryUrl, ticker)
	b := crawlers.NewBloomberg(searchTerm, ticker)
	m := crawlers.NewInvestNews(searchTerm, ticker)

	return i, b, m
}

func main() {
	db, err := OpenConn()
	if err != nil {
		log.Fatal(err)
	}

	_, _, m := getCrawlers("vale", "vale3")

	// fmt.Println("Coletando investing.com")
	// i.Run(db)

	// fmt.Println("Coletando bloomberg")
	// b.Run(db)

	fmt.Println("Coletando investnews")
	m.Run(db)
}
