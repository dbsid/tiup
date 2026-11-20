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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	perrs "github.com/pingcap/errors"
	"github.com/pingcap/tiup/pkg/cluster/api"
	"github.com/pingcap/tiup/pkg/cluster/ctxt"
	"github.com/pingcap/tiup/pkg/cluster/template/scripts"
	logprinter "github.com/pingcap/tiup/pkg/logger/printer"
	"github.com/pingcap/tiup/pkg/meta"
	"github.com/pingcap/tiup/pkg/utils"
)

// tidb-one: a tidb-one-server runs TiDB and TiKV in one process
// (github.com/dbsid/tidb-one). Each instance is a TiDB server (SQL and
// status ports, registered in etcd like a TiDB) and a TiKV store (TiKV and
// TiKV status ports, registered in PD like a TiKV), so monitoring, TiDB
// Dashboard and PD treat it as one of each.
//
// Restarting an instance (reload, upgrade, stop --evict-leader) first evicts
// the store's Region leaders, then drains it on every tidb-one router (no new
// sessions or transactions; sessions move to other servers at their next
// transaction boundary) and waits until the routers hold no connection to
// it. Scale-in drains it on the routers and takes the store offline like a
// TiKV; `prune` removes it once the store is Tombstone.

// TiDBOneSpec represents a tidb-one server in topology.yaml.
type TiDBOneSpec struct {
	Host            string               `yaml:"host"`
	ManageHost      string               `yaml:"manage_host,omitempty" validate:"manage_host:editable"`
	ListenHost      string               `yaml:"listen_host,omitempty"`
	AdvertiseAddr   string               `yaml:"advertise_address,omitempty"`
	SSHPort         int                  `yaml:"ssh_port,omitempty" validate:"ssh_port:editable"`
	IgnoreExporter  bool                 `yaml:"ignore_exporter,omitempty"`
	Port            int                  `yaml:"port" default:"4000"`
	StatusPort      int                  `yaml:"status_port" default:"10080"`
	TiKVPort        int                  `yaml:"tikv_port" default:"20160"`
	TiKVStatusPort  int                  `yaml:"tikv_status_port" default:"20180"`
	DeployDir       string               `yaml:"deploy_dir,omitempty"`
	DataDir         string               `yaml:"data_dir,omitempty"`
	LogDir          string               `yaml:"log_dir,omitempty"`
	Offline         bool                 `yaml:"offline,omitempty"`
	Source          string               `yaml:"source,omitempty" validate:"source:editable"`
	NumaNode        string               `yaml:"numa_node,omitempty" validate:"numa_node:editable"`
	NumaCores       string               `yaml:"numa_cores,omitempty" validate:"numa_cores:editable"`
	Config          map[string]any       `yaml:"config,omitempty" validate:"config:ignore"`
	TiKVConfig      map[string]any       `yaml:"tikv_config,omitempty" validate:"tikv_config:ignore"`
	ResourceControl meta.ResourceControl `yaml:"resource_control,omitempty" validate:"resource_control:editable"`
	Arch            string               `yaml:"arch,omitempty"`
	OS              string               `yaml:"os,omitempty"`
}

// Role returns the component role of the instance.
func (s *TiDBOneSpec) Role() string {
	return ComponentTiDBOne
}

// SSH returns the host and SSH port of the instance.
func (s *TiDBOneSpec) SSH() (string, int) {
	return s.GetManageHost(), s.SSHPort
}

// GetMainPort returns the main port of the instance.
func (s *TiDBOneSpec) GetMainPort() int {
	return s.Port
}

// GetManageHost returns the manage host of the instance.
func (s *TiDBOneSpec) GetManageHost() string {
	if s.ManageHost != "" {
		return s.ManageHost
	}
	return s.Host
}

// IgnoreMonitorAgent returns if the node does not have monitor agents available.
func (s *TiDBOneSpec) IgnoreMonitorAgent() bool {
	return s.IgnoreExporter
}

// SQLAddr is the advertised SQL address: what TiDB publishes in
// /info/all and what the routers drain.
func (s *TiDBOneSpec) SQLAddr() string {
	host := s.Host
	if s.AdvertiseAddr != "" {
		host = s.AdvertiseAddr
	}
	return utils.JoinHostPort(host, s.Port)
}

// StoreAddr is the address of the embedded TiKV store in PD.
func (s *TiDBOneSpec) StoreAddr() string {
	return utils.JoinHostPort(s.Host, s.TiKVPort)
}

// Status queries the state of the instance: Down if the TiDB status port
// does not answer, otherwise the state of its store in PD.
func (s *TiDBOneSpec) Status(ctx context.Context, timeout time.Duration, tlsCfg *tls.Config, pdList ...string) string {
	if statusByHost(s.GetManageHost(), s.StatusPort, "/status", timeout, tlsCfg) != "Up" {
		return "Down"
	}
	state := checkStoreStatus(ctx, s.StoreAddr(), tlsCfg, pdList...)
	if s.Offline && strings.ToLower(state) == "offline" {
		state = "Pending Offline"
	}
	return state
}

// Labels returns the store labels (tikv_config server.labels).
func (s *TiDBOneSpec) Labels() (map[string]string, error) {
	kv := &TiKVSpec{Host: s.Host, Port: s.TiKVPort, Config: s.TiKVConfig}
	return kv.Labels()
}

// TiDBOneComponent represents the tidb-one server component.
type TiDBOneComponent struct{ Topology *Specification }

// Name implements Component interface.
func (c *TiDBOneComponent) Name() string {
	return ComponentTiDBOne
}

// Role implements Component interface.
func (c *TiDBOneComponent) Role() string {
	return ComponentTiDBOne
}

// Source implements Component interface.
func (c *TiDBOneComponent) Source() string {
	return ComponentTiDBOne
}

// CalculateVersion implements the Component interface.
func (c *TiDBOneComponent) CalculateVersion(clusterVersion string) string {
	version := c.Topology.ComponentVersions.TiDBOne
	if version == "" {
		version = clusterVersion
	}
	return version
}

// SetVersion implements Component interface.
func (c *TiDBOneComponent) SetVersion(version string) {
	c.Topology.ComponentVersions.TiDBOne = version
}

// Instances implements Component interface.
func (c *TiDBOneComponent) Instances() []Instance {
	ins := make([]Instance, 0, len(c.Topology.TiDBOneServers))
	for _, s := range c.Topology.TiDBOneServers {
		ins = append(ins, &TiDBOneInstance{BaseInstance{
			InstanceSpec: s,
			Name:         c.Name(),
			Host:         s.Host,
			ManageHost:   s.ManageHost,
			ListenHost:   utils.Ternary(s.ListenHost != "", s.ListenHost, c.Topology.BaseTopo().GlobalOptions.ListenHost).(string),
			Port:         s.Port,
			SSHP:         s.SSHPort,
			Source:       s.Source,
			NumaNode:     s.NumaNode,
			NumaCores:    s.NumaCores,
			Ports: []int{
				s.Port,
				s.StatusPort,
				s.TiKVPort,
				s.TiKVStatusPort,
			},
			Dirs: []string{
				s.DeployDir,
				s.DataDir,
			},
			StatusFn: s.Status,
			UptimeFn: func(_ context.Context, timeout time.Duration, tlsCfg *tls.Config) time.Duration {
				return UptimeByHost(s.GetManageHost(), s.StatusPort, timeout, tlsCfg, "")
			},
			Component: c,
		}, c.Topology, 0})
	}
	return ins
}

// TiDBOneInstance represents a tidb-one server instance.
type TiDBOneInstance struct {
	BaseInstance
	topo                     Topology
	leaderCountBeforeRestart int
}

// InitConfig implements Instance interface.
func (i *TiDBOneInstance) InitConfig(
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
	spec := i.InstanceSpec.(*TiDBOneSpec)

	pds := []string{}
	for _, pdspec := range topo.PDServers {
		pds = append(pds, utils.JoinHostPort(pdspec.Host, pdspec.ClientPort))
	}
	cfg := &scripts.TiDBOneScript{
		Port:          spec.Port,
		StatusPort:    spec.StatusPort,
		ListenHost:    i.GetListenHost(),
		AdvertiseAddr: utils.Ternary(spec.AdvertiseAddr != "", spec.AdvertiseAddr, spec.Host).(string),
		PD:            strings.Join(pds, ","),

		TiKVAddr:                utils.JoinHostPort(i.GetListenHost(), spec.TiKVPort),
		TiKVAdvertiseAddr:       spec.StoreAddr(),
		TiKVStatusAddr:          utils.JoinHostPort(i.GetListenHost(), spec.TiKVStatusPort),
		TiKVAdvertiseStatusAddr: utils.JoinHostPort(spec.Host, spec.TiKVStatusPort),

		DeployDir: paths.Deploy,
		DataDir:   paths.Data[0],
		LogDir:    paths.Log,
		NumaNode:  spec.NumaNode,
		NumaCores: spec.NumaCores,
	}
	fp := filepath.Join(paths.Cache, fmt.Sprintf("run_tidb-one_%s_%d.sh", i.GetHost(), i.GetPort()))
	if err := cfg.ConfigToFile(fp); err != nil {
		return err
	}
	dst := filepath.Join(paths.Deploy, "scripts", "run_tidb-one.sh")
	if err := e.Transfer(ctx, fp, dst, false, 0, false); err != nil {
		return err
	}
	if _, _, err := e.Execute(ctx, "chmod +x "+dst, false); err != nil {
		return err
	}

	tidbConfig := spec.Config
	if len(topo.TiDBOneRouters) > 0 {
		// The routers move sessions between servers with SHOW / SET
		// SESSION_STATES; every server must accept the others' tokens.
		tidbConfig = copyConfig(tidbConfig)
		tidbConfig["security.session-token-signing-cert"] = filepath.Join(paths.Deploy, TLSCertKeyDir, "tiproxy-session.crt")
		tidbConfig["security.session-token-signing-key"] = filepath.Join(paths.Deploy, TLSCertKeyDir, "tiproxy-session.key")
	}
	if err := i.mergeConfigFile(ctx, e, "tidb", topo.ServerConfigs.TiDB, tidbConfig, paths); err != nil {
		return err
	}
	return i.mergeConfigFile(ctx, e, "tikv", topo.ServerConfigs.TiKV, spec.TiKVConfig, paths)
}

func copyConfig(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+2)
	for k, v := range m {
		out[k] = v
	}
	return out
}

// mergeConfigFile writes conf/<name>.toml from the global and instance
// configuration (tidb.toml for the SQL layer, tikv.toml for the store).
func (i *TiDBOneInstance) mergeConfigFile(ctx context.Context, e ctxt.Executor, name string, globalConf, instanceConf map[string]any, paths meta.DirPaths) error {
	fp := filepath.Join(paths.Cache, fmt.Sprintf("%s-%s-%s-%d.toml", i.ComponentName(), name, i.GetHost(), i.GetPort()))
	conf, err := Merge2Toml(name, globalConf, instanceConf)
	if err != nil {
		return err
	}
	if err := utils.WriteFile(fp, conf, os.ModePerm); err != nil {
		return err
	}
	return e.Transfer(ctx, fp, filepath.Join(paths.Deploy, "conf", name+".toml"), false, 0, false)
}

// ScaleConfig deploys a temporary config on scaling.
func (i *TiDBOneInstance) ScaleConfig(
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

var _ RollingUpdateInstance = &TiDBOneInstance{}

// PreRestart implements RollingUpdateInstance interface: evict the store's
// leaders, then drain the server on every router.
func (i *TiDBOneInstance) PreRestart(ctx context.Context, topo Topology, apiTimeoutSeconds int, tlsCfg *tls.Config, updcfg *UpdateConfig) error {
	tidbTopo := mustBeClusterTopo(topo)
	spec := i.InstanceSpec.(*TiDBOneSpec)
	logger := ctx.Value(logprinter.ContextKeyLogger).(*logprinter.Logger)
	timeoutOpt := &utils.RetryOption{
		Timeout: time.Second * time.Duration(apiTimeoutSeconds),
		Delay:   time.Second * 2,
	}

	if len(tidbTopo.TiDBOneServers)+len(tidbTopo.TiKVServers) > 1 {
		pdClient := api.NewPDClient(ctx, tidbTopo.GetPDListWithManageHost(), 5*time.Second, tlsCfg)
		if err := pdClient.WaitLeader(timeoutOpt); err != nil {
			return err
		}
		counter := tidbOneLeaderCounter(tidbTopo, tlsCfg)
		leaderCount, err := counter(spec.StoreAddr())
		if err != nil {
			return perrs.Annotatef(err, "failed to get leader count %s", i.ID())
		}
		i.leaderCountBeforeRestart = leaderCount
		logger.Infof("\tEvicting %d leaders from %s", leaderCount, spec.StoreAddr())
		if err := pdClient.EvictStoreLeader(spec.StoreAddr(), timeoutOpt, counter); err != nil {
			if !utils.IsTimeoutOrMaxRetry(err) {
				return perrs.Annotatef(err, "failed to evict store leader %s", i.ID())
			}
			logger.Warnf("Ignore evicting store leader from %s, %v", i.ID(), err)
		}
	}

	wait := time.Duration(apiTimeoutSeconds) * time.Second
	if wait > tidbOneDrainWait {
		wait = tidbOneDrainWait
	}
	return DrainTiDBOneOnRouters(ctx, tidbTopo, spec.SQLAddr(), true, wait, tlsCfg)
}

// PostRestart implements RollingUpdateInstance interface: open the server
// on the routers again and give its leaders back.
func (i *TiDBOneInstance) PostRestart(ctx context.Context, topo Topology, tlsCfg *tls.Config, updcfg *UpdateConfig) error {
	tidbTopo := mustBeClusterTopo(topo)
	spec := i.InstanceSpec.(*TiDBOneSpec)
	logger := ctx.Value(logprinter.ContextKeyLogger).(*logprinter.Logger)

	if err := DrainTiDBOneOnRouters(ctx, tidbTopo, spec.SQLAddr(), false, 0, tlsCfg); err != nil {
		return err
	}
	if len(tidbTopo.TiDBOneServers)+len(tidbTopo.TiKVServers) <= 1 {
		return nil
	}
	pdClient := api.NewPDClient(ctx, tidbTopo.GetPDListWithManageHost(), 5*time.Second, tlsCfg)
	if err := pdClient.RemoveStoreEvict(spec.StoreAddr()); err != nil {
		return perrs.Annotatef(err, "failed to remove evict store scheduler for %s", i.ID())
	}
	if i.leaderCountBeforeRestart > 0 {
		if err := pdClient.RecoverStoreLeader(spec.StoreAddr(), i.leaderCountBeforeRestart, nil, tidbOneLeaderCounter(tidbTopo, tlsCfg)); err != nil {
			if !utils.IsTimeoutOrMaxRetry(err) {
				return perrs.Annotatef(err, "failed to recover store leader %s", i.ID())
			}
			logger.Warnf("Ignore recovering store leader from %s, %v", i.ID(), err)
		}
	}
	return nil
}

// tidbOneLeaderCounter counts the leaders of a tidb-one store (by store
// address) from its TiKV status port.
func tidbOneLeaderCounter(topo *Specification, tlsCfg *tls.Config) func(string) (int, error) {
	return func(storeAddr string) (int, error) {
		for _, s := range topo.TiDBOneServers {
			if s.StoreAddr() == storeAddr {
				kv := &Specification{TiKVServers: []*TiKVSpec{{
					Host:       s.Host,
					ManageHost: s.ManageHost,
					Port:       s.TiKVPort,
					StatusPort: s.TiKVStatusPort,
				}}}
				return genLeaderCounter(kv, tlsCfg)(storeAddr)
			}
		}
		// A plain TiKV of the same cluster.
		return genLeaderCounter(topo, tlsCfg)(storeAddr)
	}
}

// ScaleIn drains the server on the routers and takes its store offline. The
// instance keeps running until the store is Tombstone; `prune` removes it.
func (i *TiDBOneInstance) ScaleIn(ctx context.Context, pdClient *api.PDClient, timeoutOpt *utils.RetryOption) error {
	spec := i.InstanceSpec.(*TiDBOneSpec)
	topo := mustBeClusterTopo(i.topo)
	if err := DrainTiDBOneOnRouters(ctx, topo, spec.SQLAddr(), true, tidbOneDrainWait, nil); err != nil {
		return err
	}
	return pdClient.DelStore(spec.StoreAddr(), timeoutOpt)
}
