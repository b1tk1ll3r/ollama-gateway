package state

import (
	"github.com/example/ollama-fair-gateway/internal/config"
	"path/filepath"
)

type Paths struct {
	DataDir           string
	Config            string
	APIKeys           string
	Policies          string
	Metrics           string
	Quota             string
	WorkerPerformance string
	ModelPlacement    string
	WorkerState       string
	AutoTune          string
	WarmModels        string
	Alerts            string
	Conversations     string
	BatchJobs         string
	BatchDir          string
	UsageDir          string
}

func Resolve(c config.StorageConfig) Paths {
	return Paths{
		DataDir:           c.DataDir,
		Config:            filepath.Join(c.DataDir, c.ConfigFile),
		APIKeys:           filepath.Join(c.DataDir, c.APIKeysFile),
		Policies:          filepath.Join(c.DataDir, c.PoliciesFile),
		Metrics:           filepath.Join(c.DataDir, c.MetricsFile),
		Quota:             filepath.Join(c.DataDir, c.QuotaFile),
		WorkerPerformance: filepath.Join(c.DataDir, c.WorkerPerformanceFile),
		ModelPlacement:    filepath.Join(c.DataDir, c.ModelPlacementFile),
		WorkerState:       filepath.Join(c.DataDir, c.WorkerStateFile),
		AutoTune:          filepath.Join(c.DataDir, c.AutoTuneFile),
		WarmModels:        filepath.Join(c.DataDir, c.WarmModelsFile),
		Alerts:            filepath.Join(c.DataDir, c.AlertsFile),
		Conversations:     filepath.Join(c.DataDir, c.ConversationsFile),
		BatchJobs:         filepath.Join(c.DataDir, c.BatchJobsFile),
		BatchDir:          filepath.Join(c.DataDir, c.BatchJobsDir),
		UsageDir:          filepath.Join(c.DataDir, "usage"),
	}
}
