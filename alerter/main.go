package main

import (
	"fmt"
	"github.com/certusone/chain_exporter/types"
	"github.com/getsentry/raven-go"
	"github.com/go-pg/pg"
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type (
	Monitor struct {
		db      *pg.DB
		address string
		LastBlock int64
		ConsecutiveBlocks uint8
	}
)

func main() {
	if os.Getenv("DB_HOST") == "" {
		panic(errors.New("DB_HOST needs to be set"))
	}
	if os.Getenv("DB_USER") == "" {
		panic(errors.New("DB_USER needs to be set"))
	}
	if os.Getenv("DB_PW") == "" {
		panic(errors.New("DB_PW needs to be set"))
	}
	if os.Getenv("RAVEN_DSN") == "" {
		panic(errors.New("RAVEN_DSN needs to be set"))
	}
	if os.Getenv("ADDRESS") == "" {
		panic(errors.New("ADDRESS needs to be set"))
	}

	// Set Raven URL for alerts
	raven.SetDSN(os.Getenv("RAVEN_DSN"))

	// Connect to the postgres datastore
	db := pg.Connect(&pg.Options{
		Addr:     os.Getenv("DB_HOST"),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PW"),
	})
	defer db.Close()

	// Start the monitor
	monitor := &Monitor{db, os.Getenv("ADDRESS"), int64(0), uint8(0)}

	go func() {
		for range time.Tick(30*time.Second) {
			fmt.Println("start - alerting on misses")
			err := monitor.AlertMisses()
			if err != nil {
				fmt.Printf("error - alerting on misses: %v\n", err)
			}
			fmt.Println("finish - alerting on misses")
		}
	}()
	// Allow graceful closing of the process
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
}

// AlertMisses queries misses from the database and sends the relevant alert to sentry
func (m *Monitor) AlertMisses() error {
	// Query block misses from the DB
	var misses []*types.MissInfo
	err := m.db.Model(&types.MissInfo{}).Where("alerted = FALSE and address = ?", m.address).Order("height ASC").Select(&misses)
	if err != nil {
		return err
	}

	// Iterate misses and send alerts
	for _, miss := range misses {
		if(miss.Height == m.LastBlock +  1) {
			m.ConsecutiveBlocks += 1
			fmt.Println("ConsecutiveBlocks", m.ConsecutiveBlocks)
		} else {
			m.ConsecutiveBlocks = 0
		}
		m.LastBlock = miss.Height

		if(m.ConsecutiveBlocks >= 10) {
			raven.CaptureMessage("Missed blocks", map[string]string{"height": strconv.FormatInt(miss.Height, 10), "time": miss.Time.String(), "address": miss.Address})
			m.ConsecutiveBlocks = 0
		}

		// Mark miss as alerted in the db
		miss.Alerted = true
		_, err = m.db.Model(miss).Where("id = ?", miss.ID).Update()
		if err != nil {
			return err
		}
		fmt.Printf("Processed miss #height: %d\n", miss.Height)
	}

	return nil
}
