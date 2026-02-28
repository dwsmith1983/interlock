package dynamodb

// Config holds DynamoDB connection and table settings.
type Config struct {
	TableName    string `yaml:"tableName" json:"tableName"`
	Region       string `yaml:"region" json:"region"`
	Endpoint     string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	ReadinessTTL string `yaml:"readinessTtl,omitempty" json:"readinessTtl,omitempty"`
	RetentionTTL string `yaml:"retentionTtl,omitempty" json:"retentionTtl,omitempty"`
	CreateTable  bool   `yaml:"createTable,omitempty" json:"createTable,omitempty"`
}
