package runtime

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib" // postgres driver
	"github.com/nyaruka/gocommon/aws/cwatch"
)

type Runtime struct {
	Config *Config
	DB     *sql.DB
	CW     *cwatch.Service
}
