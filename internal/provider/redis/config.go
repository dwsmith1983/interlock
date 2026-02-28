package redis

// Config holds Redis/Valkey connection and store settings.
type Config struct {
	Addr           string `yaml:"addr"`
	Password       string `yaml:"password,omitempty"`
	DB             int    `yaml:"db,omitempty"`
	KeyPrefix      string `yaml:"keyPrefix"`
	ReadinessTTL   string `yaml:"readinessTtl,omitempty" json:"readinessTtl,omitempty"`
	RetentionTTL   string `yaml:"retentionTtl,omitempty" json:"retentionTtl,omitempty"` // default "168h" (7 days)
	RunIndexLimit  int    `yaml:"runIndexLimit,omitempty" json:"runIndexLimit,omitempty"`
	EventStreamMax int64  `yaml:"eventStreamMax,omitempty" json:"eventStreamMax,omitempty"`
}
