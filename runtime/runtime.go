package runtime

import (
	"database/sql"

	"github.com/nyaruka/gocommon/aws/cwatch"
)

type Runtime struct {
	Config *Config
	DB     *sql.DB
	CW     *cwatch.Service
}
