package config

import (
	"fmt"
	"log"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// Config 结构体，用于映射配置文件
type Config struct {
	Database struct {
		DataDir       string `mapstructure:"data_dir"`
		WalDir        string `mapstructure:"wal_dir"`
		SegmentSize   int    `mapstructure:"segment_size"`
		FlushInterval int    `mapstructure:"flush_interval"`
	} `mapstructure:"database"`

	MemoryTable struct {
		MaxSize int    `mapstructure:"max_size"`
		Type    string `mapstructure:"type"`
	} `mapstructure:"memory_table"`

	Network struct {
		Address string `mapstructure:"address"`
	} `mapstructure:"network"`

	Logging struct {
		Level   string `mapstructure:"level"`
		LogFile string `mapstructure:"log_file"`
	} `mapstructure:"logging"`
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(configPath)
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("加载配置文件失败: %w", err)
	}

	var cfg Config

	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		log.Println("配置文件修改，重新加载...")
		if err := v.Unmarshal(&cfg); err != nil {
			fmt.Println(fmt.Errorf("解析配置文件失败: %w", err))
		}
	})

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	return &cfg, nil
}
