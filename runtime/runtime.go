package runtime

import (
	"database/sql"

	_ "github.com/lib/pq"
	"github.com/nyaruka/gocommon/aws/cwatch"
)

type Runtime struct {
	Config *Config
	DB     *sql.DB
	CW     *cwatch.Service
}
