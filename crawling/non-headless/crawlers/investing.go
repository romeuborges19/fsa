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

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

type Investing struct {
	queryUrl string
	ticker   string
}

func NewInvesting(queryUrl string, ticker string) *Investing {
	return &Investing{queryUrl: queryUrl, ticker: ticker}
}

func (i *Investing) collectLinks(page int, ctx context.Context, ch chan string, wg *sync.WaitGroup) {
	url := fmt.Sprintf("%s/%d", i.queryUrl, page)

	var articleNodes []*cdp.Node
	err := chromedp.Run(ctx,
		network.SetBlockedURLs([]string{"*/streaming.forexpros.com/*"}),
		chromedp.Navigate(url),
		chromedp.Sleep(1000*time.Millisecond),
		chromedp.Nodes(`article [data-test="article-title-link"]`, &articleNodes, chromedp.ByQueryAll),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, node := range articleNodes {
		url, _ := node.Attribute("href")
		ch <- url
		wg.Add(1)
	}
}

func (i *Investing) scrapArticle(link string, db *sql.DB, wg *sync.WaitGroup) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
	)

	initialCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()

	ctx, cancel := chromedp.NewContext(initialCtx)
	defer cancel()

	ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := chromedp.Run(ctx, fetch.Enable()); err != nil {
		log.Fatal(err)
	}

	chromedp.ListenTarget(ctx, func(ev any) {
		if ev, ok := ev.(*fetch.EventRequestPaused); ok {
			go func() {
				c := chromedp.FromContext(ctx)
				ctx := cdp.WithExecutor(ctx, c.Target)

				if ev.ResourceType == network.ResourceTypeImage ||
					ev.ResourceType == network.ResourceTypeStylesheet ||
					ev.ResourceType == network.ResourceTypeMedia ||
					ev.ResourceType == network.ResourceTypeScript ||
					ev.ResourceType == network.ResourceTypeFetch ||
					ev.ResourceType == network.ResourceTypeXHR {
					if err := fetch.FailRequest(ev.RequestID, network.ErrorReasonBlockedByClient).Do(ctx); err != nil {
						log.Fatal(err)
					}
				} else {
					if err := fetch.ContinueRequest(ev.RequestID).Do(ctx); err != nil {
						log.Fatal(err)
					}
				}
			}()
		}
	})
	var title string
	var articleTime string

	var articleNode []*cdp.Node
	err := chromedp.Run(ctx,
		network.SetBlockedURLs([]string{"*/streaming.forexpros.com/*"}),
		chromedp.Navigate(link),
		chromedp.Sleep(1000*time.Millisecond),
		chromedp.Text(`#articleTitle`, &title),
		chromedp.Text(`//span[text()="Publicado"]`, &articleTime),
		chromedp.Nodes(`#article`, &articleNode),
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	var articleHtml string
	for _, node := range articleNode {
		err = chromedp.Run(ctx,
			chromedp.InnerHTML(node.FullXPath(), &articleHtml),
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(articleHtml))
	if err != nil {
		log.Fatal(err)
	}

	var content string
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		text := s.Text()
		content += " " + text
	})

	articleTime = strings.TrimPrefix(articleTime, "Publicado ")
	date, err := time.Parse("02.01.2006, 15:04", articleTime)
	if err != nil {
		fmt.Printf("Erro ao parsear string: %v", err)
	}

	hash := md5.Sum([]byte(link))
	hashId := hex.EncodeToString(hash[:])

	article := &Article{
		date:    date,
		content: strings.TrimSpace(content),
		title:   title,
		url:     link,
		hashId:  hashId,
		ticker:  i.ticker,
	}
	SaveArticle(article, db)
	wg.Done()
}

func (i *Investing) Run(db *sql.DB) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
	)

	initialCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancel()

	ctx, cancel := chromedp.NewContext(initialCtx)
	defer cancel()

	if err := chromedp.Run(ctx, fetch.Enable()); err != nil {
		log.Fatal(err)
	}

	chromedp.ListenTarget(ctx, func(ev any) {
		if ev, ok := ev.(*fetch.EventRequestPaused); ok {
			go func() {
				c := chromedp.FromContext(ctx)
				ctx := cdp.WithExecutor(ctx, c.Target)

				if ev.ResourceType == network.ResourceTypeImage ||
					ev.ResourceType == network.ResourceTypeStylesheet ||
					ev.ResourceType == network.ResourceTypeMedia ||
					ev.ResourceType == network.ResourceTypeScript ||
					ev.ResourceType == network.ResourceTypeFetch ||
					ev.ResourceType == network.ResourceTypeXHR {
					if err := fetch.FailRequest(ev.RequestID, network.ErrorReasonBlockedByClient).Do(ctx); err != nil {
						log.Fatal(err)
					}
				} else {
					if err := fetch.ContinueRequest(ev.RequestID).Do(ctx); err != nil {
						log.Fatal(err)
					}
				}
			}()
		}
	})

	linkCh := make(chan string)
	var wg sync.WaitGroup

	go func() {
		defer close(linkCh)
		for j := 150; j <= 250; j++ {
			i.collectLinks(j, ctx, linkCh, &wg)
		}
	}()

	for link := range linkCh {
		go i.scrapArticle(link, db, &wg)
	}

	wg.Wait()
}
