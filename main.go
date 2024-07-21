package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/athariqk/pgcdc/logrepl"
	"github.com/athariqk/pgcdc/publishers"
	"github.com/joho/godotenv"
	"github.com/meilisearch/meilisearch-go"
)

func main() {
	env := os.Getenv("PGCDC_ENV")
	if env == "" {
		env = "development"
	}

	godotenv.Load(".env." + env + ".local")
	if env != "test" {
		godotenv.Load(".env.local")
	}
	godotenv.Load(".env." + env)
	godotenv.Load() // The Original .env

	log.Println("Environment:", env)

	outputPlugin := os.Getenv("OUTPUT_PLUGIN")
	connectionString := os.Getenv("PGSQL_CONNECTION_STRING")
	pubName := os.Getenv("PGSQL_PUB_NAME")
	slotName := os.Getenv("PGSQL_REPL_SLOT_NAME")
	standbyMessageTimeout, err := strconv.Atoi(os.Getenv("PGSQL_STANDBY_MESSAGE_TIMEOUT"))
	if err != nil {
		standbyMessageTimeout = 10
	}

	mode := logrepl.STREAM_MODE
	if len(os.Args) > 1 && os.Args[1] == "full" {
		mode = logrepl.POPULATE_MODE
	}

	pubs := []logrepl.Publisher{
		publishers.NewMeiliSyncer(meilisearch.ClientConfig{
			Host:   os.Getenv("MS_API_URL"),
			APIKey: os.Getenv("MS_API_KEY")}),
		publishers.NewNsqPublisher(
			os.Getenv("NSQD_INSTANCE_ADDRESS"),
			os.Getenv("NSQD_INSTANCE_PORT")),
	}

	replicator := logrepl.LogicalReplicator{
		OutputPlugin:          outputPlugin,
		ConnectionString:      connectionString,
		PublicationName:       pubName,
		SlotName:              slotName,
		StandbyMessageTimeout: time.Second * time.Duration(standbyMessageTimeout),
		Publishers:            pubs,
		Schema:                logrepl.NewSchema("schema.yaml"),
		Mode:                  mode,
	}

	replicator.Run()
}
