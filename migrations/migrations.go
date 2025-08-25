package migrations

import (
	"embed"
)

//go:embed *.up.sql
//go:embed *.down.sql
var FS embed.FS

const Path = "."
