#!/bin/bash
set -e

# WARNING: This file was auto-generated. Do not edit!
#          All your edit might be overwritten!
DEPLOY_DIR={{.DeployDir}}

cd "${DEPLOY_DIR}" || exit 1

# tidb-one-server: TiDB and TiKV in one process.

{{- if and .NumaNode .NumaCores}}
exec numactl --cpunodebind={{.NumaNode}} --membind={{.NumaNode}} -C {{.NumaCores}} env GODEBUG=madvdontneed=1 bin/tidb-one-server \
{{- else if .NumaNode}}
exec numactl --cpunodebind={{.NumaNode}} --membind={{.NumaNode}} env GODEBUG=madvdontneed=1 bin/tidb-one-server \
{{- else}}
exec env GODEBUG=madvdontneed=1 bin/tidb-one-server \
{{- end}}
    -P {{.Port}} \
    --status="{{.StatusPort}}" \
    --host="{{.ListenHost}}" \
    --advertise-address="{{.AdvertiseAddr}}" \
    --store="tikv" \
    --path="{{.PD}}" \
    --log-slow-query="{{.LogDir}}/tidb_slow_query.log" \
    --config=conf/tidb.toml \
    --log-file="{{.LogDir}}/tidb.log" \
    --tikv.addr="{{.TiKVAddr}}" \
    --tikv.advertise-addr="{{.TiKVAdvertiseAddr}}" \
    --tikv.status-addr="{{.TiKVStatusAddr}}" \
    --tikv.advertise-status-addr="{{.TiKVAdvertiseStatusAddr}}" \
    --tikv.config=conf/tikv.toml \
    --tikv.data-dir="{{.DataDir}}" \
    --tikv.log-file="{{.LogDir}}/tikv.log" 2>> "{{.LogDir}}/tidb-one_stderr.log"
