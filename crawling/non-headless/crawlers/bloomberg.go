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

type Bloomberg struct {
	searchTerm string
	ticker     string
}

func NewBloomberg(searchTerm string, ticker string) *Bloomberg {
	return &Bloomberg{searchTerm: searchTerm, ticker: ticker}
}

func (b *Bloomberg) readBody(ctx context.Context, nodes []*cdp.Node) string {
	var content string
	for _, node := range nodes {
		var pNodes []*cdp.Node
		chromedp.Run(ctx,
			chromedp.Nodes("p.body-paragraph", &pNodes, chromedp.FromNode(node)),
		)
		for _, p := range pNodes {
			var html string
			chromedp.Run(ctx,
				chromedp.Text(p.FullXPath(), &html),
			)
			if strings.Contains(html, "Leia também") {
				break
			}
			content += " " + strings.TrimSpace(html)
		}
	}

	return strings.TrimSpace(content)
}

func (b *Bloomberg) scrapArticle(ch chan string, db *sql.DB, wg *sync.WaitGroup) {
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

		var title, date string
		var articleNode []*cdp.Node
		chromedp.Run(ctx,
			chromedp.Navigate(url),
			chromedp.Text("h1", &title),
			chromedp.Text("small", &date),
			chromedp.Nodes("article", &articleNode, chromedp.ByQuery),
		)

		dateArr := strings.Split(date, " ")
		var dateObj time.Time
		if 2 <= len(dateArr)-1 {
			monthStr := dateArr[2]
			month := strings.TrimSuffix(monthStr, ",")
			date = strings.Replace(date, month, Months[month], 1)
			dateObj, _ = time.Parse("02 de 01, 2006 | 03:04 PM", date)
		}

		content := b.readBody(ctx, articleNode)

		article := &Article{
			title:   title,
			date:    dateObj,
			content: content,
			url:     url,
			hashId:  hashId,
			ticker:  b.ticker,
		}
		SaveArticle(article, db)
		wg.Done()
	}
}

func (b *Bloomberg) navigate(wg *sync.WaitGroup, ch chan string) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
	)

	initialCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()

	ctx, cancel := chromedp.NewContext(initialCtx)
	defer cancel()

	url := fmt.Sprintf(
		"https://www.bloomberglinea.com.br/queryly-advanced-search/?query=%s",
		b.searchTerm,
	)

	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.WaitReady("body"),
		chromedp.SetValue(`select#sortby`, "date", chromedp.ByQuery),
	)
	if err != nil {
		log.Fatal(err)
	}

	var articles []*cdp.Node
	for i := 0; i <= 250; i++ {
		fmt.Println(i)
		err = chromedp.Run(ctx,
			chromedp.Nodes(".queryly_item_row a", &articles, chromedp.ByQueryAll),
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
			chromedp.Evaluate(`document.querySelector('a.next_btn').click()`, nil),
			chromedp.Sleep(3*time.Second),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (b *Bloomberg) Run(db *sql.DB) {
	var wg sync.WaitGroup
	ch := make(chan string)

	for w := 0; w <= 3; w++ {
		fmt.Println("boneco", w)
		go b.scrapArticle(ch, db, &wg)
	}

	b.navigate(&wg, ch)
	defer close(ch)

	wg.Wait()
}
