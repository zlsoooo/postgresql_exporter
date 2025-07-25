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
	"fmt"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	
    "github.com/go-kit/log"
    "github.com/go-kit/log/level"
)

// Server describes a connection to Postgres.
// Also it contains metrics map and query overrides.
type Server struct {
	db          *sql.DB
	labels      prometheus.Labels
	master      bool
	runonserver string

	// Last version used to calculate metric map. If mismatch on scrape,
	// then maps are recalculated.
	lastMapVersion semver.Version
	// Currently active metric map
	metricMap map[string]MetricMapNamespace
	// Currently active query overrides
	queryOverrides map[string]string
	mappingMtx     sync.RWMutex
	// Currently cached metrics
	metricCache map[string]cachedMetrics
	cacheMtx    sync.Mutex
}

// ServerOpt configures a server.
type ServerOpt func(*Server)

// ServerWithLabels configures a set of labels.
func ServerWithLabels(labels prometheus.Labels) ServerOpt {
	return func(s *Server) {
		for k, v := range labels {
			s.labels[k] = v
		}
	}
}

// NewServer establishes a new connection using DSN.
func NewServer(dsn string, opts ...ServerOpt) (*Server, error) {
	fingerprint, err := parseFingerprint(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	logger.Info("Established new database connection", "fingerprint", fingerprint)

	s := &Server{
		db:     db,
		master: false,
		labels: prometheus.Labels{
			serverLabelName: fingerprint,
		},
		metricCache: make(map[string]cachedMetrics),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// Close disconnects from Postgres.
func (s *Server) Close() error {
	return s.db.Close()
}

// Ping checks connection availability and possibly invalidates the connection if it fails.
func (s *Server) Ping() error {
	if s.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	if err := s.db.Ping(); err != nil {
		if cerr := s.Close(); cerr != nil {
			logger.Error("Error while closing non-pinging DB connection", "server", s, "err", cerr)
		}
		return err
	}
	return nil
}


// String returns server's fingerprint.
func (s *Server) String() string {
	return s.labels[serverLabelName]
}

// Scrape loads metrics.
func (s *Server) Scrape(ch chan<- prometheus.Metric, disableSettingsMetrics bool) error {
	s.mappingMtx.RLock()
	defer s.mappingMtx.RUnlock()

	var err error

	if !disableSettingsMetrics && s.master {
		if err = querySettings(ch, s); err != nil {
			return fmt.Errorf("error retrieving settings: %s", err)
		}
	}

	// DB 연결 체크 먼저 실행
	if err := s.Ping(); err != nil {
		// DB down → fallback metric 수집
		for namespace, mapping := range s.metricMap {
			fallback := fallbackMetrics(namespace, mapping)
			for _, m := range fallback {
				ch <- m
			}
		}
		// Scrape 자체는 성공으로 간주
		return nil
	}

	// DB 연결되었을 경우 정상 쿼리 실행
	errMap := queryNamespaceMappings(ch, s)
	if len(errMap) == 0 {
		return nil
	}

	for namespace := range errMap {
		if mapping, ok := s.metricMap[namespace]; ok {
			fallback := fallbackMetrics(namespace, mapping)
			for _, metric := range fallback {
				ch <- metric
			}
		}
	}

	var Logger = log.NewNopLogger() 

	err = fmt.Errorf("queryNamespaceMappings errors encountered")
	for namespace, errStr := range errMap {
		// 로그로 에러 출력
		level.Error(Logger).Log("msg", "Namespace query failed", "namespace", namespace, "err", errStr)

		// fallbackMetrics 호출하여 기본값(-1) 메트릭 수집
		if mapping, ok := s.metricMap[namespace]; ok {
			fallback := fallbackMetrics(namespace, mapping)
			for _, metric := range fallback {
				ch <- metric
			}
		}

		// 오류 누적
		err = fmt.Errorf("%s, namespace: %s error: %s", err, namespace, errStr)
	}

	return err
}



// Servers contains a collection of servers to Postgres.
type Servers struct {
	m       sync.Mutex
	servers map[string]*Server
	opts    []ServerOpt
}

// NewServers creates a collection of servers to Postgres.
func NewServers(opts ...ServerOpt) *Servers {
	return &Servers{
		servers: make(map[string]*Server),
		opts:    opts,
	}
}

// GetServer returns established connection from a collection.
func (s *Servers) GetServer(dsn string) (*Server, error) {
	s.m.Lock()
	defer s.m.Unlock()
	var err error
	var ok bool
	errCount := 0 // start at zero because we increment before doing work
	retries := 1
	var server *Server
	for {
		if errCount++; errCount > retries {
			return nil, err
		}
		server, ok = s.servers[dsn]
		if !ok {
			server, err = NewServer(dsn, s.opts...)
			if err != nil {
				time.Sleep(time.Duration(errCount) * time.Second)
				continue
			}
			s.servers[dsn] = server
		}
		if err = server.Ping(); err != nil {
			server.Close()
			delete(s.servers, dsn)
			time.Sleep(time.Duration(errCount) * time.Second)
			continue
		}
		break
	}
	return server, nil
}

// Close disconnects from all known servers.
func (s *Servers) Close() {
	s.m.Lock()
	defer s.m.Unlock()
	for _, server := range s.servers {
		if err := server.Close(); err != nil {
			logger.Error("Failed to close connection", "server", server, "err", err)
		}
	}
}
