// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus-community/postgres_exporter/collector"
	"github.com/prometheus-community/postgres_exporter/config"
	"github.com/prometheus/client_golang/prometheus"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/promslog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/prometheus/exporter-toolkit/web/kingpinflag"
)

var (
	c = config.Handler{
		Config: &config.Config{},
	}

	configFile             = kingpin.Flag("config.file", "Postgres exporter configuration file.").Default("postgres_exporter.yml").String()
	webConfig              = kingpinflag.AddFlags(kingpin.CommandLine, ":9187")
	metricsPath            = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").Envar("PG_EXPORTER_WEB_TELEMETRY_PATH").String()
	disableDefaultMetrics  = kingpin.Flag("disable-default-metrics", "Do not include default metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_DEFAULT_METRICS").Bool()
	disableSettingsMetrics = kingpin.Flag("disable-settings-metrics", "Do not include pg_settings metrics.").Default("false").Envar("PG_EXPORTER_DISABLE_SETTINGS_METRICS").Bool()
	autoDiscoverDatabases  = kingpin.Flag("auto-discover-databases", "Whether to discover the databases on a server dynamically. (DEPRECATED)").Default("false").Envar("PG_EXPORTER_AUTO_DISCOVER_DATABASES").Bool()
	queriesPath            = kingpin.Flag("extend.query-path", "Path to custom queries to run. (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXTEND_QUERY_PATH").String()
	onlyDumpMaps           = kingpin.Flag("dumpmaps", "Do not run, simply dump the maps.").Bool()
	constantLabelsList     = kingpin.Flag("constantLabels", "A list of label=value separated by comma(,). (DEPRECATED)").Default("").Envar("PG_EXPORTER_CONSTANT_LABELS").String()
	excludeDatabases       = kingpin.Flag("exclude-databases", "A list of databases to remove when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_EXCLUDE_DATABASES").String()
	includeDatabases       = kingpin.Flag("include-databases", "A list of databases to include when autoDiscoverDatabases is enabled (DEPRECATED)").Default("").Envar("PG_EXPORTER_INCLUDE_DATABASES").String()
	metricPrefix           = kingpin.Flag("metric-prefix", "A metric prefix can be used to have non-default (not \"pg\") prefixes for each of the metrics").Default("pg").Envar("PG_EXPORTER_METRIC_PREFIX").String()
	logger                 = promslog.NewNopLogger()
)

// Metric name parts.
const (
	// Namespace for all metrics.
	namespace = "pg"
	// Subsystems.
	exporter = "exporter"
	// The name of the exporter.
	exporterName = "postgres_exporter"
	// Metric label used for static string data thats handy to send to Prometheus
	// e.g. version
	staticLabelName = "static"
	// Metric label used for server identification.
	serverLabelName = "server"
)
type FallbackCollector struct{}

func (fc *FallbackCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- prometheus.NewDesc(
        "pg_exporter_status",
        "PostgreSQL exporter status (1=up, 0=down)",
        []string{"error"}, nil)
}

func (fc *FallbackCollector) Collect(ch chan<- prometheus.Metric) {
    ch <- prometheus.MustNewConstMetric(
        prometheus.NewDesc(
            "pg_exporter_status",
            "PostgreSQL exporter status (1=up, 0=down)",
            []string{"error"}, nil),
        prometheus.GaugeValue, 0, "connection failed",
    )
}

func main() {
	kingpin.Version(version.Print(exporterName))
	promslogConfig := &promslog.Config{}
	flag.AddFlags(kingpin.CommandLine, promslogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger = promslog.New(promslogConfig)

	if *onlyDumpMaps {
		dumpMaps()
		return
	}

	if err := c.ReloadConfig(*configFile, logger); err != nil {
		// This is not fatal, but it means that auth must be provided for every dsn.
		logger.Warn("Error loading config", "err", err)
	}

	dsns, err := getDataSources()
	if err != nil {
		logger.Error("Failed reading data sources", "err", err.Error())
		os.Exit(1)
	}

	excludedDatabases := strings.Split(*excludeDatabases, ",")
	logger.Info("Excluded databases", "databases", fmt.Sprintf("%v", excludedDatabases))

	if *queriesPath != "" {
		logger.Warn("The extended queries.yaml config is DEPRECATED", "file", *queriesPath)
	}

	if *autoDiscoverDatabases || *excludeDatabases != "" || *includeDatabases != "" {
		logger.Warn("Scraping additional databases via auto discovery is DEPRECATED")
	}

	if *constantLabelsList != "" {
		logger.Warn("Constant labels on all metrics is DEPRECATED")
	}

	opts := []ExporterOpt{
		DisableDefaultMetrics(*disableDefaultMetrics),
		DisableSettingsMetrics(*disableSettingsMetrics),
		AutoDiscoverDatabases(*autoDiscoverDatabases),
		WithUserQueriesPath(*queriesPath),
		WithConstantLabels(*constantLabelsList),
		ExcludeDatabases(excludedDatabases),
		IncludeDatabases(*includeDatabases),
	}

	exporter := NewExporter(dsns, opts...)
	defer func() {
		exporter.servers.Close()
	}()

	prometheus.MustRegister(versioncollector.NewCollector(exporterName))

	prometheus.MustRegister(exporter)

	prometheus.MustRegister(&FallbackCollector{})


	// TODO(@sysadmind): Remove this with multi-target support. We are removing multiple DSN support
	dsn := ""
	if len(dsns) > 0 {
		dsn = dsns[0]
	}

	pe, err := collector.NewPostgresCollector(
		logger,
		excludedDatabases,
		dsn,
		[]string{},
	)
	if err != nil {
		logger.Warn("Failed to create PostgresCollector", "err", err.Error())
	} else {
		prometheus.MustRegister(pe)
	}

	http.Handle(*metricsPath, promhttp.Handler())

	if *metricsPath != "/" && *metricsPath != "" {
		landingConfig := web.LandingConfig{
			Name:        "Postgres Exporter",
			Description: "Prometheus PostgreSQL server Exporter",
			Version:     version.Info(),
			Links: []web.LandingLinks{
				{
					Address: *metricsPath,
					Text:    "Metrics",
				},
			},
		}
		landingPage, err := web.NewLandingPage(landingConfig)
		if err != nil {
			logger.Error("error creating landing page", "err", err)
			os.Exit(1)
		}
		http.Handle("/", landingPage)
	}

	http.HandleFunc("/probe", handleProbe(logger, excludedDatabases))

	srv := &http.Server{}
	if err := web.ListenAndServe(srv, webConfig, logger); err != nil {
		logger.Error("Error running HTTP server", "err", err)
		os.Exit(1)
	}
}
