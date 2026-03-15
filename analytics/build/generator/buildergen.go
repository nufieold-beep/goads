//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
)

const (
	modulesDir = "../modules"
	tmplName   = "builder.tmpl"
	outName    = "builder.go"
)

type moduleData struct {
	Key   string
	Dir   string
	Alias string
}

type templateData struct {
	Modules []moduleData
	Imports []moduleData
}

func main() {
	modules := make([]moduleData, 0)
	err := filepath.WalkDir(modulesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() || path == modulesDir {
			return nil
		}

		moduleDir := filepath.Base(path)
		moduleFile := filepath.Join(path, "module.go")
		if _, err := os.Stat(moduleFile); err == nil {
			modules = append(modules, moduleData{Key: moduleKey(moduleDir), Dir: moduleDir, Alias: sanitizeIdent(moduleDir)})
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("walk error: %v", err))
	}

	sort.Slice(modules, func(i, j int) bool { return modules[i].Key < modules[j].Key })
	data := templateData{Modules: modules, Imports: modules}
	t, err := template.New(tmplName).ParseFiles(filepath.Join("generator", tmplName))
	if err != nil {
		panic(fmt.Sprintf("failed to parse builder template: %s", err))
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(fmt.Sprintf("failed to generate %s content: %s", outName, err))
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		panic(fmt.Errorf("failed to format generated code: %w\n---\n%s", err, buf.String()))
	}

	if err := os.WriteFile(outName, formatted, 0o644); err != nil {
		panic(fmt.Sprintf("failed to write %s: %s", outName, err))
	}

	fmt.Printf("%s file successfully generated\n", outName)
}

func sanitizeIdent(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

func moduleKey(dir string) string {
	if dir == "filelogger" {
		return "file"
	}
	return dir
}