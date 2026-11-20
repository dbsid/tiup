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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiup/pkg/cluster/template/config"
	"github.com/pingcap/tiup/pkg/cluster/template/scripts"
	logprinter "github.com/pingcap/tiup/pkg/logger/printer"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const tidbOneTopo = `
global:
  user: "tidb"
  deploy_dir: "/tidb-deploy"
  data_dir: "/tidb-data"
pd_servers:
  - host: 10.0.1.1
tidb_one_servers:
  - host: 10.0.1.1
  - host: 10.0.1.2
    port: 4001
    tikv_port: 20161
    tikv_config:
      server.labels: { host: h2 }
tidb_one_routers:
  - host: 10.0.1.3
    config:
      refresh-ms: 500
      no-pipeline-begin: true
monitoring_servers:
  - host: 10.0.1.3
`

func TestTiDBOneTopology(t *testing.T) {
	topo := &Specification{}
	require.NoError(t, yaml.Unmarshal([]byte(tidbOneTopo), topo))
	require.NoError(t, topo.Validate())

	one := topo.TiDBOneServers[0]
	require.Equal(t, 4000, one.Port)
	require.Equal(t, 10080, one.StatusPort)
	require.Equal(t, 20160, one.TiKVPort)
	require.Equal(t, 20180, one.TiKVStatusPort)
	require.Equal(t, "/tidb-deploy/tidb-one-4000", one.DeployDir)
	require.Equal(t, "/tidb-data/tidb-one-4000", one.DataDir)
	require.Equal(t, "10.0.1.1:4000", one.SQLAddr())
	require.Equal(t, "10.0.1.1:20160", one.StoreAddr())
	labels, err := topo.TiDBOneServers[1].Labels()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"host": "h2"}, labels)

	r := topo.TiDBOneRouters[0]
	require.Equal(t, 6000, r.Port)
	require.Equal(t, 3080, r.StatusPort)
	require.Equal(t, []string{"--no-pipeline-begin", "--refresh-ms 500"}, routerFlags(r.Config))

	// Start order: PD, then the servers, then the routers.
	var order []string
	for _, c := range topo.ComponentsByStartOrder() {
		if len(c.Instances()) > 0 {
			order = append(order, c.Name())
		}
	}
	require.Equal(t, []string{ComponentPD, ComponentTiDBOne, ComponentTiDBOneRouter, ComponentPrometheus}, order)
}

func TestTiDBOnePortConflict(t *testing.T) {
	topo := &Specification{}
	err := yaml.Unmarshal([]byte(`
pd_servers:
  - host: 10.0.1.1
tidb_one_servers:
  - host: 10.0.1.1
  - host: 10.0.1.1
    port: 4001
    status_port: 10081
`), topo)
	require.Error(t, err)
	require.Contains(t, err.Error(), "port conflict")
}

func TestTiDBOneScripts(t *testing.T) {
	dir := t.TempDir()
	run := filepath.Join(dir, "run.sh")
	require.NoError(t, (&scripts.TiDBOneScript{
		Port: 4000, StatusPort: 10080, ListenHost: "0.0.0.0", AdvertiseAddr: "10.0.1.1",
		PD:       "10.0.1.1:2379",
		TiKVAddr: "0.0.0.0:20160", TiKVAdvertiseAddr: "10.0.1.1:20160",
		TiKVStatusAddr: "0.0.0.0:20180", TiKVAdvertiseStatusAddr: "10.0.1.1:20180",
		DeployDir: "/d", DataDir: "/data", LogDir: "/d/log",
	}).ConfigToFile(run))
	b, err := os.ReadFile(run)
	require.NoError(t, err)
	s := string(b)
	for _, want := range []string{
		"bin/tidb-one-server", `--path="10.0.1.1:2379"`, "--config=conf/tidb.toml",
		`--tikv.advertise-addr="10.0.1.1:20160"`, "--tikv.config=conf/tikv.toml", `--tikv.data-dir="/data"`,
	} {
		require.Contains(t, s, want)
	}

	require.NoError(t, (&scripts.TiDBOneRouterScript{
		DeployDir: "/r", LogDir: "/r/log", Listen: "0.0.0.0:6000", Status: "0.0.0.0:3080",
		TiDBStatus: "10.0.1.1:10080,10.0.1.2:10080", PD: "10.0.1.1:2379", ExtraFlags: "--refresh-ms 500",
	}).ConfigToFile(run))
	b, err = os.ReadFile(run)
	require.NoError(t, err)
	s = string(b)
	require.Contains(t, s, "bin/tiproxy-rs locality-router")
	require.Contains(t, s, `--tidb-status "10.0.1.1:10080,10.0.1.2:10080"`)
	require.Contains(t, s, "--refresh-ms 500 >>")
}

func TestTiDBOnePrometheus(t *testing.T) {
	c := config.NewPrometheusConfig("c1", "v9.0.0", false)
	c.AddTiDB("10.0.1.1", 10080).AddTiKV("10.0.1.1", 20180).AddTiDBOneRouter("10.0.1.3", 3080)
	b, err := c.Config()
	require.NoError(t, err)
	s := string(b)
	require.Contains(t, s, `job_name: "tidb-one-router"`)
	require.Contains(t, s, "'10.0.1.3:3080'")
	require.Contains(t, s, "'10.0.1.1:20180'")

	// No router: no job.
	b, err = config.NewPrometheusConfig("c1", "v9.0.0", false).Config()
	require.NoError(t, err)
	require.NotContains(t, string(b), "tidb-one-router")
}

// fakeRouter serves the drain API of a tidb-one router.
type fakeRouter struct {
	mu    sync.Mutex
	calls []string
	conns int64
}

func (f *fakeRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/backends/"):
		f.calls = append(f.calls, strings.TrimPrefix(r.URL.Path, "/api/backends/")+" "+r.URL.Query().Get("addr"))
		_, _ = w.Write([]byte("[]"))
	case r.URL.Path == "/api/backends":
		n := f.conns
		if f.conns > 0 {
			f.conns-- // sessions move away over time
		}
		_, _ = w.Write([]byte(`[{"addr":"10.0.1.1:4000","draining":true,"connections":` + itoa(n) + `},{"addr":"10.0.1.2:4000","connections":9}]`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestDrainTiDBOneOnRouters(t *testing.T) {
	f := &fakeRouter{conns: 3}
	srv := httptest.NewServer(f)
	defer srv.Close()
	host, port := splitHostPort(t, srv.Listener.Addr().String())
	topo := &Specification{TiDBOneRouters: []*TiDBOneRouterSpec{
		{Host: host, StatusPort: port},
		{Host: "127.0.0.1", StatusPort: 1}, // down: skipped with a warning
	}}
	ctx := context.WithValue(context.Background(), logprinter.ContextKeyLogger, logprinter.NewLogger(""))

	start := time.Now()
	require.NoError(t, DrainTiDBOneOnRouters(ctx, topo, "10.0.1.1:4000", true, 10*time.Second, nil))
	require.Less(t, time.Since(start), 5*time.Second, "returns once the connections are gone")
	require.NoError(t, DrainTiDBOneOnRouters(ctx, topo, "10.0.1.1:4000", false, 0, nil))
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []string{"drain 10.0.1.1:4000", "resume 10.0.1.1:4000"}, f.calls)
	require.EqualValues(t, 0, f.conns)
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	i := strings.LastIndex(addr, ":")
	port, err := strconv.Atoi(addr[i+1:])
	require.NoError(t, err)
	return addr[:i], port
}
