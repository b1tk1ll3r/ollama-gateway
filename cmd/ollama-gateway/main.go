package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/ollama-fair-gateway/internal/alerts"
	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/autotune"
	"github.com/example/ollama-fair-gateway/internal/batch"
	"github.com/example/ollama-fair-gateway/internal/conversation"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/infrastructure"
	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	"github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/server"
	"github.com/example/ollama-fair-gateway/internal/session"
	"github.com/example/ollama-fair-gateway/internal/state"
	"github.com/example/ollama-fair-gateway/internal/telemetry"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/warm"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func main() {
	configPath := flag.String("config", "config.json", "configuration file")
	checkConfig := flag.Bool("check-config", false, "validate effective configuration and storage, then exit")
	probeURL := flag.String("probe", "", "probe an HTTP health/readiness URL, then exit")
	probeTimeout := flag.Duration("probe-timeout", 2*time.Second, "timeout for -probe")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	if *probeURL != "" {
		if err := runProbe(*probeURL, *probeTimeout); err != nil {
			log.Error("probe failed", "url", *probeURL, "error", err)
			os.Exit(1)
		}
		return
	}
	if *checkConfig {
		if err := runConfigCheck(*configPath, os.Stdout); err != nil {
			log.Error("configuration preflight failed", "error", err)
			os.Exit(2)
		}
		return
	}
	loaded, err := loadEffectiveConfig(*configPath)
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(2)
	}
	cfg := loaded.Config
	paths := loaded.Paths
	configStore := loaded.Store
	if loaded.Persistent {
		log.Info("loaded persistent UI configuration", "path", configStore.Path())
	}
	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	apiKeyStore, err := state.NewAPIKeyStore(paths.APIKeys)
	if err != nil {
		log.Error("API key store initialization failed", "error", err)
		os.Exit(2)
	}
	authenticator, err := auth.NewWithRuntimeStore(rootCtx, cfg.Auth, apiKeyStore)
	if err != nil {
		log.Error("authentication initialization failed", "error", err)
		os.Exit(2)
	}

	sched := scheduler.NewLocal(cfg.Scheduler.GlobalConcurrency, cfg.Scheduler.MaxQueue, cfg.Scheduler.MaxQueuePerActor)
	var ledger quota.Ledger = quota.Disabled{}
	var quotaMemory *quota.Memory
	if cfg.Quota.Enabled {
		quotaMemory = quota.NewMemory()
		if err := quotaMemory.LoadPersistent(paths.Quota); err != nil {
			log.Error("quota state initialization failed", "error", err)
			os.Exit(2)
		}
		quotaMemory.StartPersistence(rootCtx, paths.Quota, cfg.Storage.FlushInterval.Value(), func(err error) { log.Error("quota persistence failed", "error", err) })
		ledger = quotaMemory
	}
	policyStore, err := state.NewPolicyStore(paths.Policies)
	if err != nil {
		log.Error("policy store initialization failed", "error", err)
		os.Exit(2)
	}
	placementStore, err := state.NewModelPlacementStore(paths.ModelPlacement)
	if err != nil {
		log.Error("model placement store initialization failed", "error", err)
		os.Exit(2)
	}
	workerStateStore, err := state.NewWorkerRuntimeStore(paths.WorkerState)
	if err != nil {
		log.Error("worker state store initialization failed", "error", err)
		os.Exit(2)
	}
	uiSessions := session.NewMemory()
	conversationStore, err := conversation.New(cfg.Conversations, paths.Conversations)
	if err != nil {
		log.Error("conversation store initialization failed", "error", err)
		os.Exit(2)
	}
	conversationStore.StartCleanup(rootCtx, func(err error) { log.Error("conversation retention cleanup failed", "error", err) })
	batchManager, err := batch.New(cfg.BatchJobs, paths.BatchJobs, paths.BatchDir)
	if err != nil {
		log.Error("batch job manager initialization failed", "error", err)
		os.Exit(2)
	}
	pool := worker.New(cfg.Workers, cfg.Native.ControlWorker)
	pool.SetRoutingConfig(cfg.Routing)
	pool.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	pool.SetReliabilityConfig(cfg.Reliability)
	if overrides, err := placementStore.List(rootCtx); err != nil {
		log.Error("model placement overrides load failed", "error", err)
		os.Exit(2)
	} else {
		for workerName, rule := range overrides {
			if err := pool.SetPlacement(workerName, rule, true); err != nil {
				// Keep orphaned rules durable when a worker is temporarily removed;
				// they become active again if that worker name returns later.
				log.Warn("ignoring model placement override for unknown worker", "worker", workerName, "error", err)
			}
		}
	}
	if modes, err := workerStateStore.List(rootCtx); err != nil {
		log.Error("worker runtime state load failed", "error", err)
		os.Exit(2)
	} else {
		for name, mode := range modes {
			if err := pool.SetMaintenance(name, mode); err != nil {
				log.Warn("ignoring worker state for unknown worker", "worker", name, "error", err)
			}
		}
	}
	if err := pool.LoadPerformance(paths.WorkerPerformance); err != nil {
		log.Error("worker performance state initialization failed", "error", err)
		os.Exit(2)
	}
	pool.StartPerformancePersistence(rootCtx, paths.WorkerPerformance, cfg.Storage.FlushInterval.Value(), func(err error) { log.Error("worker performance persistence failed", "error", err) })
	tuner, err := autotune.New(cfg.AutoTuning, pool, paths.AutoTune)
	if err != nil {
		log.Error("auto tuning state initialization failed", "error", err)
		os.Exit(2)
	}
	for workerName, models := range tuner.Applied() {
		for model, limit := range models {
			if err := pool.SetModelConcurrency(workerName, model, limit); err != nil {
				log.Warn("ignoring auto-tune override", "worker", workerName, "model", model, "error", err)
			}
		}
	}
	pool.Start(rootCtx)
	warmManager, err := warm.New(cfg.WarmModels, pool, paths.WarmModels)
	if err != nil {
		log.Error("warm model manager initialization failed", "error", err)
		os.Exit(2)
	}
	warmManager.Start(rootCtx)
	alertManager, err := alerts.New(cfg.Alerts, paths.Alerts, func() alerts.Snapshot {
		st := sched.Stats(context.Background())
		ws := pool.Snapshots()
		aw := make([]alerts.Worker, 0, len(ws))
		for _, w := range ws {
			aw = append(aw, alerts.Worker{Name: w.Name, Healthy: w.Healthy, CircuitState: w.CircuitState, LastCircuitError: w.LastCircuitError, VRAMUsedBytes: w.VRAMUsedBytes, VRAMTotalBytes: w.VRAMTotalBytes})
		}
		return alerts.Snapshot{QueueDepth: int(st.Queued), QueueWait: st.OldestWait, Workers: aw, StorageBytes: alerts.DirSize(paths.DataDir)}
	})
	if err != nil {
		log.Error("alerts manager initialization failed", "error", err)
		os.Exit(2)
	}
	alertManager.Start(rootCtx)
	live := liveflow.New(10*time.Second, max(512, cfg.Infrastructure.MaxRequests))
	infra := infrastructure.New(cfg.Infrastructure, live, sched, pool)
	infra.Start(rootCtx)
	met := metrics.New()
	if err := met.LoadPersistent(paths.Metrics); err != nil {
		log.Error("metrics state initialization failed", "error", err)
		os.Exit(2)
	}
	met.StartPersistence(rootCtx, paths.Metrics, cfg.Storage.FlushInterval.Value(), func(err error) { log.Error("metrics persistence failed", "error", err) })
	rec, err := usage.NewWithRetention(cfg.Usage.JournalDir, cfg.Usage.Buffer, cfg.Usage.FlushInterval.Value(), usage.RetentionConfig{DetailDays: cfg.Usage.Retention.DetailDays, DailyDays: cfg.Usage.Retention.DailyDays, MonthlyMonths: cfg.Usage.Retention.MonthlyMonths, CompactionInterval: cfg.Usage.Retention.CompactionInterval.Value()}, met.DropUsage)
	if err != nil {
		log.Error("usage recorder initialization failed", "error", err)
		os.Exit(2)
	}
	rec.SetRecentCapacity(cfg.UI.RecentEvents)
	defer rec.Close()
	otelExporter := telemetry.New(cfg.OpenTelemetry)
	if otelExporter != nil {
		defer func() {
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelExporter.Close(cctx); err != nil {
				log.Warn("OpenTelemetry shutdown", "error", err)
			}
		}()
	}
	srvHandler := server.New(cfg, server.Dependencies{Auth: authenticator, Scheduler: sched, Quota: ledger, Estimator: cost.New(cfg.Cost), Workers: pool, Proxy: proxy.New(), Usage: rec, Metrics: met, Policies: policyStore, Sessions: uiSessions, Live: live, Infrastructure: infra, Logger: log, ConfigStore: configStore, PlacementStore: placementStore, WorkerStateStore: workerStateStore, AutoTune: tuner, OpenTelemetry: otelExporter, WarmModels: warmManager, Alerts: alertManager, Conversations: conversationStore, BatchJobs: batchManager})
	batchManager.Start(rootCtx, srvHandler.ExecuteBatch)
	hs := &http.Server{Addr: cfg.Server.Listen, Handler: srvHandler.Handler(), ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Value(), IdleTimeout: cfg.Server.IdleTimeout.Value(), MaxHeaderBytes: 1 << 20}
	go func() {
		<-rootCtx.Done()
		ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = hs.Shutdown(ctx)
	}()
	log.Info("ollama fair gateway starting", "listen", cfg.Server.Listen, "workers", len(cfg.Workers), "coordination", "in-memory", "node", infra.NodeName(), "quota", cfg.Quota.Enabled, "storage", cfg.Storage.DataDir)
	if cfg.Server.TLSCert != "" || cfg.Server.TLSKey != "" {
		err = hs.ListenAndServeTLS(cfg.Server.TLSCert, cfg.Server.TLSKey)
	} else {
		err = hs.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		log.Error("server failed", "error", err)
		os.Exit(1)
	}
	if warmManager != nil {
		wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := warmManager.Wait(wctx); err != nil {
			log.Warn("warm model actions still active during shutdown", "error", err)
		}
		wcancel()
	}
	if batchManager != nil {
		bctx, bcancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := batchManager.Wait(bctx); err != nil {
			log.Warn("batch attempts still active during shutdown", "error", err)
		}
		bcancel()
	}
	rec.Close()
	if quotaMemory != nil {
		if err := quotaMemory.SavePersistent(paths.Quota); err != nil {
			log.Error("final quota persistence failed", "error", err)
		}
	}
	if err := met.SavePersistent(paths.Metrics); err != nil {
		log.Error("final metrics persistence failed", "error", err)
	}
	if err := pool.SavePerformance(paths.WorkerPerformance); err != nil {
		log.Error("final worker performance persistence failed", "error", err)
	}
	log.Info("gateway stopped")
}
