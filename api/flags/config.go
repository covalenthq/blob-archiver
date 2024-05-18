package flags

import (
	"fmt"

	common "github.com/base-org/blob-archiver/common/flags"
	oplog "github.com/ethereum-optimism/optimism/op-service/log"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/urfave/cli/v2"
)

type APIConfig struct {
	LogConfig      oplog.CLIConfig
	MetricsConfig  opmetrics.CLIConfig
	BeaconConfig   common.BeaconConfig
	StorageConfig  common.StorageConfig
	UpstreamConfig common.UpstreamConfig

	ListenAddr string
}

func (c APIConfig) Check() error {
	if err := c.StorageConfig.Check(); err != nil {
		return fmt.Errorf("storage config check failed: %w", err)
	}

	if err := c.BeaconConfig.Check(); err != nil {
		return fmt.Errorf("beacon config check failed: %w", err)
	}

	if c.ListenAddr == "" {
		return fmt.Errorf("listen address must be set")
	}

	return nil
}

func ReadConfig(cliCtx *cli.Context) APIConfig {
	return APIConfig{
		LogConfig:      oplog.ReadCLIConfig(cliCtx),
		MetricsConfig:  opmetrics.ReadCLIConfig(cliCtx),
		BeaconConfig:   common.NewBeaconConfig(cliCtx),
		StorageConfig:  common.NewStorageConfig(cliCtx),
		UpstreamConfig: common.NewUpstreamConfig(cliCtx),
		ListenAddr:     cliCtx.String(ListenAddressFlag.Name),
	}
}
