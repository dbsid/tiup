#!/bin/bash
set -e

# WARNING: This file was auto-generated. Do not edit!
#          All your edit might be overwritten!
DEPLOY_DIR={{.DeployDir}}
cd "${DEPLOY_DIR}" || exit 1

# TiProxy locality router for tidb-one.
{{- if .NumaNode}}
exec numactl --cpunodebind={{.NumaNode}} --membind={{.NumaNode}} bin/tiproxy-rs locality-router \
{{- else}}
exec bin/tiproxy-rs locality-router \
{{- end}}
    --listen "{{.Listen}}" \
    --status "{{.Status}}" \
    --tidb-status "{{.TiDBStatus}}" \
    --pd "{{.PD}}"{{if .ExtraFlags}} \
    {{.ExtraFlags}}{{end}} >> "{{.LogDir}}/tidb-one-router.log" 2>&1
