package utils

import (
	"log"

	"github.com/spf13/viper"
)

const (
	TempPath      = "/tmp/bgptools-combine"
	BaseChunkPath = TempPath + "/chunks"
)

type Config struct {
	MinioIP          string `mapstructure:"MINIO_ENDPOINT_IP"`
	MinioPort        string `mapstructure:"MINIO_ENDPOINT_PORT"`
	MinioAccessKey   string `mapstructure:"MINIO_ACCESS_KEY"`
	MinioSecertKey   string `mapstructure:"MINIO_SECRET_KEY"`
	MinioBucketName  string `mapstructure:"MINIO_BUCKET_NAME"`
	ElasticUrl       string `mapstructure:"ELASTIC_URL"`
	ElasticApikey    string `mapstructure:"ELASTIC_APIKEY"`
	ElasticIndex     string `mapstructure:"ELASTIC_INDEX"`
	NumberDeliveries int    `mapstructure:"NUMBER_DELIVERIES"`
	BgptoolsUrl      string `mapstructure:"BGPTOOLS_URL"`
	MmdbUrl          string `mapstructure:"MMDB_URL"`
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) LoadConfig(path string) error {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}
	if err := viper.Unmarshal(&c); err != nil {
		log.Fatal(err)
	}

	if err := EnsureDir(); err != nil {
		return err
	}
	return nil
}
