package runtime

import "database/sql"

type Runtime struct {
	Config *Config
	DB     *sql.DB
}
