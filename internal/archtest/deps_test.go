// Package archtest guarda a regra de dependência da arquitetura hexagonal:
// domínio e casos de uso não podem conhecer banco, HTTP nem adapter.
package archtest

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/MiguelSanzBr/outbox-gateway-demo/internal/"

// imports proibidos por camada (prefixo). Teste não entra: ele pode montar adapter de verdade.
var rules = map[string][]string{
	"domain": {"net/http", "database/sql", "github.com/jackc/", module + "adapters", module + "app"},
	"app":    {"net/http", "database/sql", "github.com/jackc/", module + "adapters"},
}

func TestDependencyRule(t *testing.T) {
	for layer, forbidden := range rules {
		files, err := filepath.Glob(filepath.Join("..", layer, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		checked := 0
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			checked++
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			ast, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range ast.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				for _, bad := range forbidden {
					if strings.HasPrefix(path, bad) {
						t.Errorf("%s importa %s: %s não pode depender de %s", f, path, layer, bad)
					}
				}
			}
		}
		// pasta renomeada ou vazia faria o teste passar sem checar nada
		if checked == 0 {
			t.Errorf("camada %s sem nenhum arquivo .go", layer)
		}
	}
}
