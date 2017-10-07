package main

import (
	"fmt"
	"log"

	"github.com/codegangsta/cli"
	"github.com/unigraph/rdb"
)

func init() {
	app.Commands = append(app.Commands, cli.Command{
		Name:   "compact",
		Usage:  "compact rdb database",
		Action: compactDb,
	})

}

func compactDb(c *cli.Context) error {
	dbName := c.GlobalString("db")
	if dbName == "" {
		cli.ShowAppHelp(c)
		return nil
	}

	dbOptions := rdb.NewDefaultOptions()
	dbOptions.SetCreateIfMissing(true)
	defaultFlags.setOptions(dbOptions, c)

	db, err := rdb.OpenDb(dbOptions, dbName)
	if err != nil {
		log.Fatal(err)
	}
	db.CompactRange(rdb.Range{})
	db.Flush(rdb.NewDefaultFlushOptions())
	fmt.Println(db.GetProperty("rocksdb.stats"))
	fmt.Println("done")
	return nil
}
