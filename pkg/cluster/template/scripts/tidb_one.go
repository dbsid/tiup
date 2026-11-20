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

package scripts

import (
	"bytes"
	"path"
	"text/template"

	"github.com/pingcap/tiup/embed"
	"github.com/pingcap/tiup/pkg/utils"
)

// TiDBOneScript represents the data to generate the tidb-one-server run script.
type TiDBOneScript struct {
	Port          int
	StatusPort    int
	ListenHost    string
	AdvertiseAddr string
	PD            string

	TiKVAddr                string
	TiKVAdvertiseAddr       string
	TiKVStatusAddr          string
	TiKVAdvertiseStatusAddr string

	DeployDir string
	DataDir   string
	LogDir    string

	NumaNode  string
	NumaCores string
}

// ConfigToFile writes the run script to file.
func (c *TiDBOneScript) ConfigToFile(file string) error {
	return renderScript("run_tidb-one.sh.tpl", c, file)
}

// TiDBOneRouterScript represents the data to generate the tidb-one router run script.
type TiDBOneRouterScript struct {
	DeployDir  string
	LogDir     string
	NumaNode   string
	Listen     string
	Status     string
	TiDBStatus string
	PD         string
	ExtraFlags string
}

// ConfigToFile writes the run script to file.
func (c *TiDBOneRouterScript) ConfigToFile(file string) error {
	return renderScript("run_tidb-one-router.sh.tpl", c, file)
}

func renderScript(name string, data any, file string) error {
	tpl, err := embed.ReadTemplate(path.Join("templates", "scripts", name))
	if err != nil {
		return err
	}
	tmpl, err := template.New(name).Parse(string(tpl))
	if err != nil {
		return err
	}
	content := bytes.NewBufferString("")
	if err := tmpl.Execute(content, data); err != nil {
		return err
	}
	return utils.WriteFile(file, content.Bytes(), 0755)
}
