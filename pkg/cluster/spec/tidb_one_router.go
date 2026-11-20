// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spec

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	perrs "github.com/pingcap/errors"
	"github.com/pingcap/tiup/pkg/cluster/ctxt"
	"github.com/pingcap/tiup/pkg/cluster/template/config"
	"github.com/pingcap/tiup/pkg/cluster/template/scripts"
	logprinter "github.com/pingcap/tiup/pkg/logger/printer"
	"github.com/pingcap/tiup/pkg/meta"
	"github.com/pingcap/tiup/pkg/set"
	"github.com/pingcap/tiup/pkg/utils"
)

// tidb-one router: the TiProxy locality router (`tiproxy-rs
// locality-router`). It routes each transaction to the tidb-one server next
// to the Region leader it touches and moves sessions between servers at
// transaction boundaries. Its status port serves metrics and the backend
// drain API used before a tidb-one server restarts.

// tidbOneDrainWait caps how long a restart waits for the routers to move all
// sessions off a server. Sessions that cannot move (open transaction,
// temporary tables, locks) keep their connection until the server stops.
const tidbOneDrainWait = 60 * time.Second

// TiDBOneRouterSpec represents a tidb-one router in topology.yaml.
type TiDBOneRouterSpec struct {
	Host       string `yaml:"host"`
	ManageHost string `yaml:"manage_host,omitempty" validate:"manage_host:editable"`
	SSHPort    int    `yaml:"ssh_port,omitempty" validate:"ssh_port:editable"`
	Port       int    `yaml:"port" default:"6000"`
	StatusPort int    `yaml:"status_port" default:"3080"`
	DeployDir  string `yaml:"deploy_dir,omitempty"`
	LogDir     string `yaml:"log_dir,omitempty"`
	NumaNode   string `yaml:"numa_node,omitempty" validate:"numa_node:editable"`
	// Config holds extra router flags: `refresh-ms: 500` becomes
	// `--refresh-ms 500`, `no-session-track: true` becomes `--no-session-track`.
	Config map[string]any `yaml:"config,omitempty" validate:"config:ignore"`
	Arch   string         `yaml:"arch,omitempty"`
	OS     string         `yaml:"os,omitempty"`
}

// Role returns the component role of the instance.
func (s *TiDBOneRouterSpec) Role() string {
	return ComponentTiDBOneRouter
}

// SSH returns the host and SSH port of the instance.
func (s *TiDBOneRouterSpec) SSH() (string, int) {
	return s.GetManageHost(), s.SSHPort
}

// GetMainPort returns the main port of the instance.
func (s *TiDBOneRouterSpec) GetMainPort() int {
	return s.Port
}

// GetManageHost returns the manage host of the instance.
func (s *TiDBOneRouterSpec) GetManageHost() string {
	if s.ManageHost != "" {
		return s.ManageHost
	}
	return s.Host
}

// IgnoreMonitorAgent returns if the node does not have monitor agents available.
func (s *TiDBOneRouterSpec) IgnoreMonitorAgent() bool {
	return false
}

// TiDBOneRouterComponent represents the tidb-one router component.
type TiDBOneRouterComponent struct{ Topology *Specification }

// Name implements Component interface.
func (c *TiDBOneRouterComponent) Name() string {
	return ComponentTiDBOneRouter
}

// Role implements Component interface.
func (c *TiDBOneRouterComponent) Role() string {
	return ComponentTiDBOneRouter
}

// Source implements Component interface.
func (c *TiDBOneRouterComponent) Source() string {
	return ComponentTiDBOneRouter
}

// CalculateVersion implements the Component interface.
func (c *TiDBOneRouterComponent) CalculateVersion(clusterVersion string) string {
	version := c.Topology.ComponentVersions.TiDBOneRouter
	if version == "" {
		version = clusterVersion
	}
	return version
}

// SetVersion implements Component interface.
func (c *TiDBOneRouterComponent) SetVersion(version string) {
	c.Topology.ComponentVersions.TiDBOneRouter = version
}

// Instances implements Component interface.
func (c *TiDBOneRouterComponent) Instances() []Instance {
	ins := make([]Instance, 0, len(c.Topology.TiDBOneRouters))
	for _, s := range c.Topology.TiDBOneRouters {
		ins = append(ins, &TiDBOneRouterInstance{BaseInstance{
			InstanceSpec: s,
			Name:         c.Name(),
			Host:         s.Host,
			ManageHost:   s.ManageHost,
			ListenHost:   c.Topology.BaseTopo().GlobalOptions.ListenHost,
			Port:         s.Port,
			SSHP:         s.SSHPort,
			NumaNode:     s.NumaNode,
			Ports:        []int{s.Port, s.StatusPort},
			Dirs:         []string{s.DeployDir},
			StatusFn: func(_ context.Context, timeout time.Duration, tlsCfg *tls.Config, _ ...string) string {
				return statusByHost(s.GetManageHost(), s.StatusPort, "/api/backends", timeout, nil)
			},
			UptimeFn: func(_ context.Context, timeout time.Duration, tlsCfg *tls.Config) time.Duration {
				return 0
			},
			Component: c,
		}, c.Topology})
	}
	return ins
}

// TiDBOneRouterInstance represents a tidb-one router instance.
type TiDBOneRouterInstance struct {
	BaseInstance
	topo Topology
}

// routerFlags renders the extra router flags in a stable order.
func routerFlags(cfg map[string]any) []string {
	keys := make([]string, 0, len(cfg))
	for k := range cfg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var flags []string
	for _, k := range keys {
		switch v := cfg[k].(type) {
		case bool:
			if v {
				flags = append(flags, "--"+k)
			}
		default:
			flags = append(flags, fmt.Sprintf("--%s %v", k, v))
		}
	}
	return flags
}

// InitConfig implements Instance interface.
func (i *TiDBOneRouterInstance) InitConfig(
	ctx context.Context,
	e ctxt.Executor,
	clusterName,
	clusterVersion,
	deployUser string,
	paths meta.DirPaths,
) error {
	topo := i.topo.(*Specification)
	if err := i.BaseInstance.InitConfig(ctx, e, topo.GlobalOptions, deployUser, paths); err != nil {
		return err
	}
	if topo.GlobalOptions.TLSEnabled {
		return perrs.New("tidb-one-router does not support TLS")
	}
	spec := i.InstanceSpec.(*TiDBOneRouterSpec)

	var statuses, pds []string
	for _, s := range topo.TiDBOneServers {
		statuses = append(statuses, utils.JoinHostPort(s.Host, s.StatusPort))
	}
	for _, pd := range topo.PDServers {
		pds = append(pds, utils.JoinHostPort(pd.Host, pd.ClientPort))
	}
	cfg := &scripts.TiDBOneRouterScript{
		DeployDir:  paths.Deploy,
		LogDir:     paths.Log,
		NumaNode:   spec.NumaNode,
		Listen:     utils.JoinHostPort(i.GetListenHost(), spec.Port),
		Status:     utils.JoinHostPort(i.GetListenHost(), spec.StatusPort),
		TiDBStatus: strings.Join(statuses, ","),
		PD:         strings.Join(pds, ","),
		ExtraFlags: strings.Join(routerFlags(spec.Config), " "),
	}
	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_tidb-one-router_%s_%d.sh", i.GetHost(), i.GetPort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(paths.Deploy, "scripts", "run_tidb-one-router.sh")
	if err := e.Transfer(ctx, fp, dst, false, 0, false); err != nil {
		return err
	}
	_, _, err := e.Execute(ctx, "chmod +x "+dst, false)
	return err
}

// ScaleConfig deploys a temporary config on scaling.
func (i *TiDBOneRouterInstance) ScaleConfig(
	ctx context.Context,
	e ctxt.Executor,
	topo Topology,
	clusterName,
	clusterVersion,
	deployUser string,
	paths meta.DirPaths,
) error {
	s := i.topo
	defer func() { i.topo = s }()
	i.topo = mustBeClusterTopo(topo)
	return i.InitConfig(ctx, e, clusterName, clusterVersion, deployUser, paths)
}

// routerBackend is one entry of a router's GET /api/backends.
type routerBackend struct {
	Addr        string `json:"addr"`
	Draining    bool   `json:"draining"`
	Connections int64  `json:"connections"`
}

// DrainTiDBOneOnRouters drains (drain = true) or reopens the tidb-one server
// with SQL address sqlAddr on every router of the cluster. When draining with
// wait > 0 it waits until no router holds a connection to the server, and
// warns if some still do after wait. A router that does not answer is
// skipped with a warning: it will notice a stopped server by itself.
func DrainTiDBOneOnRouters(ctx context.Context, topo *Specification, sqlAddr string, drain bool, wait time.Duration, tlsCfg *tls.Config) error {
	if len(topo.TiDBOneRouters) == 0 {
		return nil
	}
	logger := ctx.Value(logprinter.ContextKeyLogger).(*logprinter.Logger)
	client := utils.NewHTTPClient(5*time.Second, nil)
	action := "resume"
	if drain {
		action = "drain"
	}
	var reachable []string
	for _, r := range topo.TiDBOneRouters {
		status := utils.JoinHostPort(r.GetManageHost(), r.StatusPort)
		u := fmt.Sprintf("http://%s/api/backends/%s?addr=%s", status, action, url.QueryEscape(sqlAddr))
		if _, code, err := client.PostWithStatusCode(ctx, u, nil); err != nil || code != 200 {
			logger.Warnf("\tRouter %s: %s %s failed (HTTP %d): %v", status, action, sqlAddr, code, err)
			continue
		}
		reachable = append(reachable, status)
	}
	if !drain || wait <= 0 || len(reachable) == 0 {
		if drain {
			logger.Infof("\tDrained %s on %d router(s)", sqlAddr, len(reachable))
		}
		return nil
	}
	deadline := time.Now().Add(wait)
	for {
		left := int64(0)
		for _, status := range reachable {
			body, err := client.Get(ctx, fmt.Sprintf("http://%s/api/backends", status))
			if err != nil {
				continue
			}
			var list []routerBackend
			if err := json.Unmarshal(body, &list); err != nil {
				continue
			}
			for _, b := range list {
				if b.Addr == sqlAddr {
					left += b.Connections
				}
			}
		}
		if left == 0 {
			logger.Infof("\tDrained %s on %d router(s); no router connection left", sqlAddr, len(reachable))
			return nil
		}
		if time.Now().After(deadline) {
			logger.Warnf("\t%d router connection(s) to %s did not move within %s (open transactions or unmigratable sessions); they end when the server stops", left, sqlAddr, wait)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// addTiDBOneTargets adds the Prometheus targets of the tidb-one components.
// A server is scraped as a TiDB and as a TiKV, so the stock TiDB and TiKV
// dashboards cover it; the routers get their own job.
func addTiDBOneTargets(cfig *config.PrometheusConfig, topo Topology, hosts set.StringSet) {
	cluster, ok := topo.(*Specification)
	if !ok {
		return
	}
	for _, one := range cluster.TiDBOneServers {
		hosts.Insert(one.Host)
		cfig.AddTiDB(one.Host, uint64(one.StatusPort))
		cfig.AddTiKV(one.Host, uint64(one.TiKVStatusPort))
	}
	for _, r := range cluster.TiDBOneRouters {
		hosts.Insert(r.Host)
		cfig.AddTiDBOneRouter(r.Host, uint64(r.StatusPort))
	}
}
