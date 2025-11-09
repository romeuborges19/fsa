package crawlers

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/chromedp"
)

type InvestNews struct {
	searchTerm string
	ticker     string
}

func NewInvestNews(searchTerm string, ticker string) *InvestNews {
	return &InvestNews{searchTerm: searchTerm, ticker: ticker}
}

func (m *InvestNews) readBody(ctx context.Context, nodes []*cdp.Node) string {
	var content string
	for _, p := range nodes {
		var html string
		chromedp.Run(ctx,
			chromedp.Text(p.FullXPath(), &html),
		)
		if strings.Contains(html, "Traduzido do inglês por") {
			break
		}
		content += " " + strings.TrimSpace(html)
	}

	return strings.TrimSpace(content)
}

func (m *InvestNews) scrapArticle(ch chan string, db *sql.DB, wg *sync.WaitGroup) {
	for url := range ch {
		hash := md5.Sum([]byte(url))
		hashId := hex.EncodeToString(hash[:])

		if CheckHashIdExists(hashId, db) {
			wg.Done()
			continue
		}

		opts := append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.Flag("headless", false),
		)

		initialCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
		defer cancel()

		ctx, cancel := chromedp.NewContext(initialCtx)
		defer cancel()

		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		var title string
		var pNodes, date []*cdp.Node
		chromedp.Run(ctx,
			chromedp.Navigate(url),
			chromedp.Text("h1.title", &title),
			chromedp.Nodes("time", &date, chromedp.ByQuery),
			chromedp.Nodes("div.post-content p", &pNodes, chromedp.ByQueryAll),
		)

		content := m.readBody(ctx, pNodes)

		dateObj := m.getDate(date)

		article := &Article{
			title:   title,
			date:    dateObj,
			content: content,
			url:     url,
			hashId:  hashId,
			ticker:  m.ticker,
		}
		SaveArticle(article, db)
		wg.Done()
	}
}

func (m *InvestNews) getDate(date []*cdp.Node) time.Time {
	var dateObj time.Time
	for _, p := range date {
		datetime, _ := p.Attribute("datetime")
		// 2025-10-16T14:12:42-03:00
		dateObj, err := time.Parse("2006-01-02T15:04:05-07:00", datetime)
		if err != nil {
			log.Fatal(err)
		}

		return dateObj
	}
	return dateObj
}

func (m *InvestNews) navigate(wg *sync.WaitGroup, ch chan string) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
	)

	initialCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()

	ctx, cancel := chromedp.NewContext(initialCtx)
	defer cancel()

	url := fmt.Sprintf("https://investnews.com.br/?s=%s", m.searchTerm)

	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.WaitReady("body"),
	)
	if err != nil {
		log.Fatal(err)
	}
	var articles []*cdp.Node
	for i := 0; i <= 250; i++ {
		fmt.Println(i)
		err = chromedp.Run(ctx,
			chromedp.Nodes("a.tag-post", &articles, chromedp.ByQueryAll),
		)
		if err != nil {
			log.Fatal(err)
		}

		for _, node := range articles {
			url, _ := node.Attribute("href")
			ch <- url
			wg.Add(1)
		}
		err = chromedp.Run(ctx,
			chromedp.WaitReady("body"),
			chromedp.Evaluate(`document.querySelector('a.next').click()`, nil),
			chromedp.Sleep(3*time.Second),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (m *InvestNews) Run(db *sql.DB) {
	var wg sync.WaitGroup
	ch := make(chan string)

	for w := 0; w <= 5; w++ {
		go m.scrapArticle(ch, db, &wg)
	}

	m.navigate(&wg, ch)
	defer close(ch)

	wg.Wait()
}
