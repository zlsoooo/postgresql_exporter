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
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/blang/semver/v4"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

// Query within a namespace mapping and emit metrics. Returns fatal errors if
// the scrape fails, and a slice of errors if they were non-fatal.
func queryNamespaceMapping(server *Server, namespace string, mapping MetricMapNamespace) ([]prometheus.Metric, []error, error) {
	query, found := server.queryOverrides[namespace]

	if query == "" && found {
		return []prometheus.Metric{}, []error{}, nil
	}

	var rows *sql.Rows
	var err error

	if !found {
		rows, err = server.db.Query(fmt.Sprintf("SELECT * FROM %s;", namespace))
	} else {
		rows, err = server.db.Query(query)
	}
	if err != nil {
		return fallbackMetrics(namespace, mapping), nil, nil
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving column list for: ", namespace, err))
	}

	columnIdx := make(map[string]int, len(columnNames))
	for i, n := range columnNames {
		columnIdx[n] = i
	}

	columnData := make([]interface{}, len(columnNames))
	scanArgs := make([]interface{}, len(columnNames))
	for i := range columnData {
		scanArgs[i] = &columnData[i]
	}

	nonfatalErrors := []error{}
	metrics := make([]prometheus.Metric, 0)
	hasRow := false

	for rows.Next() {
		hasRow = true
		err = rows.Scan(scanArgs...)
		if err != nil {
			return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		}

		labels := make([]string, len(mapping.labels))
		for idx, label := range mapping.labels {
			labels[idx], _ = dbToString(columnData[columnIdx[label]])
		}

		for idx, columnName := range columnNames {
			var metric prometheus.Metric
			if metricMapping, ok := mapping.columnMappings[columnName]; ok {
				if metricMapping.discard {
					continue
				}

				if metricMapping.histogram {
					var keys []float64
					err = pq.Array(&keys).Scan(columnData[idx])
					if err != nil {
						return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving", columnName, "buckets:", namespace, err))
					}

					var values []int64
					valuesIdx, ok := columnIdx[columnName+"_bucket"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_bucket")))
						continue
					}
					err = pq.Array(&values).Scan(columnData[valuesIdx])
					if err != nil {
						return []prometheus.Metric{}, []error{}, errors.New(fmt.Sprintln("Error retrieving", columnName, "bucket values:", namespace, err))
					}

					buckets := make(map[float64]uint64, len(keys))
					for i, key := range keys {
						if i >= len(values) {
							break
						}
						buckets[key] = uint64(values[i])
					}

					idx, ok = columnIdx[columnName+"_sum"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_sum")))
						continue
					}
					sum, ok := dbToFloat64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName+"_sum", columnData[idx])))
						continue
					}

					idx, ok = columnIdx[columnName+"_count"]
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Missing column: ", namespace, columnName+"_count")))
						continue
					}
					count, ok := dbToUint64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName+"_count", columnData[idx])))
						continue
					}

					metric = prometheus.MustNewConstHistogram(metricMapping.desc, count, sum, buckets, labels...)
				} else {
					value, ok := dbToFloat64(columnData[idx])
					if !ok {
						nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName, columnData[idx])))
						continue
					}
					metric = prometheus.MustNewConstMetric(metricMapping.desc, metricMapping.vtype, value, labels...)
				}
			} else {
				desc := prometheus.NewDesc(
					fmt.Sprintf("%s_%s", namespace, columnName),
					fmt.Sprintf("Unknown metric from %s", namespace),
					mapping.labels,
					server.labels,
				)

				value, ok := dbToFloat64(columnData[idx])
				if !ok {
					nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unparseable column type - discarding: ", namespace, columnName, err)))
					continue
				}
				metric = prometheus.MustNewConstMetric(desc, prometheus.UntypedValue, value, labels...)
			}
			metrics = append(metrics, metric)
		}
	}

	if !hasRow {
		fallback := fallbackMetrics(namespace, mapping)
		metrics = append(metrics, fallback...)
	}

	return metrics, nonfatalErrors, nil
}


// Iterate through all the namespace mappings in the exporter and run their
// queries.
func queryNamespaceMappings(ch chan<- prometheus.Metric, server *Server) map[string]error {
	// Return a map of namespace -> errors
	namespaceErrors := make(map[string]error)

	scrapeStart := time.Now()

	for namespace, mapping := range server.metricMap {
		logger.Debug("Querying namespace", "namespace", namespace)

		if mapping.master && !server.master {
			logger.Debug("Query skipped...")
			continue
		}

		// check if the query is to be run on specific database server version range or not
		if len(server.runonserver) > 0 {
			serVersion, _ := semver.Parse(server.lastMapVersion.String())
			runServerRange, _ := semver.ParseRange(server.runonserver)
			if !runServerRange(serVersion) {
				logger.Debug("Query skipped for this database version", "version", server.lastMapVersion.String(), "target_version", server.runonserver)
				continue
			}
		}

		scrapeMetric := false
		// Check if the metric is cached
		server.cacheMtx.Lock()
		cachedMetric, found := server.metricCache[namespace]
		server.cacheMtx.Unlock()
		// If found, check if needs refresh from cache
		if found {
			if scrapeStart.Sub(cachedMetric.lastScrape).Seconds() > float64(mapping.cacheSeconds) {
				scrapeMetric = true
			}
		} else {
			scrapeMetric = true
		}

		var metrics []prometheus.Metric
		var nonFatalErrors []error
		var err error
		if scrapeMetric {
			metrics, nonFatalErrors, err = queryNamespaceMapping(server, namespace, mapping)
		} else {
			metrics = cachedMetric.metrics
		}

		// Serious error - a namespace disappeared
		if err != nil {
			namespaceErrors[namespace] = err
			logger.Info("error finding namespace", "err", err)
		}
		// Non-serious errors - likely version or parsing problems.
		if len(nonFatalErrors) > 0 {
			for _, err := range nonFatalErrors {
				logger.Info("error querying namespace", "err", err)
			}
		}

		// Emit the metrics into the channel
		for _, metric := range metrics {
			ch <- metric
		}

		if scrapeMetric {
			// Only cache if metric is meaningfully cacheable
			if mapping.cacheSeconds > 0 {
				server.cacheMtx.Lock()
				server.metricCache[namespace] = cachedMetrics{
					metrics:    metrics,
					lastScrape: scrapeStart,
				}
				server.cacheMtx.Unlock()
			}
		}
	}

	return namespaceErrors
}

func fallbackMetrics(namespace string, mapping MetricMapNamespace) []prometheus.Metric {
    labels := make([]string, len(mapping.labels))
    for i, label := range mapping.labels {
        switch label {
        case "node_id":
            labels[i] = "0"
        case "node_name":
            labels[i] = "no-node"
        case "type":
            labels[i] = "unknown"
        case "location":
            labels[i] = ""
        case "error":
            labels[i] = "postgresql is down"
        default:
            labels[i] = "unknown"
        }
    }
    metrics := make([]prometheus.Metric, 0)
    for _, metricMapping := range mapping.columnMappings {
        if metricMapping.discard || metricMapping.histogram {
            continue
        }

        // 안전하게 metric 생성 여부 확인
        m, err := prometheus.NewConstMetric(metricMapping.desc, metricMapping.vtype, -1, labels...)
        if err != nil {
            logger.Error("메트릭 생성 실패", "desc", metricMapping.desc.String(), "error", err)
            continue
        }
        metrics = append(metrics, m)
    }
    return metrics
}


