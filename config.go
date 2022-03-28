package indexer

import "os"

type Config struct {
	ElasticURL string `help:"the url for our elastic search instance"`
	DB         string `help:"the connection string for our database"`
	Index      string `help:"the alias for our contact index"`
	Poll       int    `help:"the number of seconds to wait between checking for updated contacts"`
	Rebuild    bool   `help:"whether to rebuild the index, swapping it when complete, then exiting (default false)"`
	Cleanup    bool   `help:"whether to remove old indexes after a rebuild"`
	LogLevel   string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN  string `help:"the sentry configuration to log errors to, if any"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`
	InstanceName    string `help:"the unique name of this instance used for analytics"`
}

func NewDefaultConfig() *Config {
	hostname, _ := os.Hostname()

	return &Config{
		ElasticURL:   "http://localhost:9200",
		DB:           "postgres://localhost/temba?sslmode=disable",
		Index:        "contacts",
		Poll:         5,
		Rebuild:      false,
		Cleanup:      false,
		LogLevel:     "info",
		InstanceName: hostname,
	}
}
