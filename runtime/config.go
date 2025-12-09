package runtime

type Config struct {
	ElasticURL string `help:"the url for our elastic search instance"`
	DB         string `help:"the connection string for our database"`
	Poll       int    `help:"the number of seconds to wait between checking for database updates"`
	Rebuild    bool   `help:"whether to rebuild the index, swapping it when complete, then exiting (default false)"`
	Cleanup    bool   `help:"whether to remove old indexes after a rebuild"`
	LogLevel   string `help:"the log level, one of error, warn, info, debug"`
	SentryDSN  string `help:"the sentry configuration to log errors to, if any"`

	AWSAccessKeyID     string `help:"access key ID to use for AWS services"`
	AWSSecretAccessKey string `help:"secret access key to use for AWS services"`
	AWSRegion          string `help:"region to use for AWS services, e.g. us-east-1"`

	CloudwatchNamespace string `help:"the namespace to use for cloudwatch metrics"`
	DeploymentID        string `help:"the deployment identifier to use for metrics"`

	ContactsIndex    string `help:"the alias to use for the contact index"`
	ContactsShards   int    `help:"the number of shards to use for the contacts index"`
	ContactsReplicas int    `help:"the number of replicas to use for the contacts index"`
}

func NewDefaultConfig() *Config {
	return &Config{
		ElasticURL: "http://elastic:9200",
		DB:         "postgres://postgres/temba?sslmode=disable",
		Poll:       5,
		Rebuild:    false,
		Cleanup:    false,
		LogLevel:   "info",

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",
		AWSRegion:          "us-east-1",

		CloudwatchNamespace: "Temba/Indexer",
		DeploymentID:        "dev",

		ContactsIndex:    "contacts",
		ContactsShards:   2,
		ContactsReplicas: 1,
	}
}
