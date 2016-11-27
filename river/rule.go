package river

import (
	"github.com/siddontang/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-mysql/schema"
)

// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	Type   string `toml:"type"`
	Parent string `toml:"parent"`

	IDColumns    string `toml:"id_columns"`
	InsertAction string `toml:"insert_action"`
	InsertScript string `toml:"insert_script"`
	UpdateAction string `toml:"update_action"`
	UpdateScript string `toml:"update_script"`
	DeleteAction string `toml:"delete_action"`
	DeleteScript string `toml:"delete_script"`

	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Index = table
	r.Type = table
	r.FieldMapping = make(map[string]string)

	return r
}

func (r *Rule) prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	if len(r.InsertAction) == 0 {
		r.InsertAction = elastic.ActionIndex
	}

	if len(r.UpdateAction) == 0 {
		r.UpdateAction = elastic.ActionUpdate
	}

	if len(r.DeleteAction) == 0 {
		r.DeleteAction = elastic.ActionDelete
	}

	return nil
}
