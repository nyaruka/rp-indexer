package contacts

import (
	_ "embed"
	"encoding/json"
)

// settings and mappings for our index
//go:embed index_settings.json
var IndexSettings json.RawMessage
