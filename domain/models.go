package domain

type Tenant struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	BearerToken string     `json:"bearer_token,omitempty"`
	Datasets    []*Dataset `json:"datasets"`
}

type Dataset struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	TenantID         string            `json:"tenant_id"`
	DataHint         string            `json:"data_hint,omitempty"` // Data format hint for processing pipeline
	ProcessingConfig *ProcessingConfig `json:"processing_config,omitempty"`
}

type ProcessingConfig struct {
	EnableRawStorage   bool   `json:"enable_raw_storage"`            // Enable raw data storage (always true for receiver)
	PartitioningScheme string `json:"partitioning_scheme,omitempty"` // S3 partitioning: "date", "date_hour", "none"
}

type S3Destination struct {
	BucketName string `mapstructure:"bucket_name" json:"bucket_name"`
	Prefix     string `mapstructure:"prefix" json:"prefix"`
	Region     string `mapstructure:"region" json:"region"`
	AccessKey  string `mapstructure:"access_key" json:"access_key"`
	SecretKey  string `mapstructure:"secret_key" json:"secret_key"`
	Endpoint   string `mapstructure:"endpoint" json:"endpoint"`
	Ssl        bool   `mapstructure:"ssl" json:"ssl"`
	UseIamRole bool   `mapstructure:"use_iam_role" json:"use_iam_role"`
}
