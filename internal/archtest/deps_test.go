// Package archtest guarda a regra de dependência da arquitetura hexagonal pelo fecho
// transitivo dos imports (go list -deps), com uma lista do q cada pacote pode usar.
// Pacote novo sem regra falha: cada dependência entre camadas é uma decisão escrita aqui.
package archtest

import (
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/MiguelSanzBr/outbox-gateway-demo/"

type pkg struct {
	ImportPath string
	Imports    []string // diretos
	Deps       []string // fecho transitivo
}

type rule struct {
	pkg   string   // relativo ao módulo; vale também pros subpacotes
	allow []string // o q pode aparecer no fecho fora da stdlib: pacote do módulo ou prefixo externo
	core  bool     // domínio e casos de uso: sem rede, banco nem processo, e sem I/O direto
	any   bool     // raiz de composição: pode tudo
}

var rules = []rule{
	{pkg: "internal/domain", core: true},
	{pkg: "internal/app", allow: []string{"internal/domain"}, core: true},
	{pkg: "internal/adapters/postgres", allow: []string{"internal/app", "internal/domain", "github.com/jackc/", "golang.org/x/"}},
	{pkg: "internal/adapters/provider", allow: []string{"internal/domain"}},
	// httpapi usa o sse: a rota do stream e o formato do evento, q é o mesmo na lista
	{pkg: "internal/adapters/httpapi", allow: []string{"internal/app", "internal/domain", "internal/adapters/sse"}},
	{pkg: "internal/adapters/sse", allow: []string{"internal/app", "internal/domain"}},
	{pkg: "internal/adapters/worker", allow: []string{"internal/app", "internal/domain"}},
	{pkg: "internal/adapters/mockprovider", allow: []string{"internal/domain"}},
	{pkg: "internal/testdb", allow: []string{"internal/adapters/postgres", "internal/app", "internal/domain", "github.com/jackc/", "golang.org/x/"}},
	{pkg: "internal/archtest"},
	{pkg: "cmd/", any: true},
}

// coreDenyDeps: nem de longe (fecho). coreDenyImports: nem direto (fmt puxa os, e tudo bem).
var (
	coreDenyDeps    = []string{"net", "database/", "os/exec", "os/signal", "crypto/tls"}
	coreDenyImports = []string{"os", "log", "io/fs"}
)

func stdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func matches(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")
}

func ruleFor(rel string) (rule, bool) {
	for _, r := range rules {
		if matches(rel, r.pkg) {
			return r, true
		}
	}
	return rule{}, false
}

// violations confere um pacote contra a regra dele.
func violations(p pkg) []string {
	rel := strings.TrimPrefix(p.ImportPath, module)
	r, ok := ruleFor(rel)
	if !ok {
		return []string{rel + ": pacote sem regra em archtest"}
	}
	if r.any {
		return nil
	}
	var out []string
	for _, d := range p.Deps {
		switch {
		case strings.HasPrefix(d, module):
			dep := strings.TrimPrefix(d, module)
			if matches(dep, r.pkg) {
				continue
			}
			allowed := false
			for _, a := range r.allow {
				allowed = allowed || matches(dep, a)
			}
			if !allowed {
				out = append(out, rel+" depende de "+dep)
			}
		case stdlib(d):
			for _, bad := range coreDenyDeps {
				if r.core && (matches(d, bad) || strings.HasPrefix(d, bad)) {
					out = append(out, rel+" depende de "+d+" (rede, banco ou processo)")
				}
			}
		default:
			allowed := false
			for _, a := range r.allow {
				allowed = allowed || strings.HasPrefix(d, a)
			}
			if !allowed {
				out = append(out, rel+" depende de "+d)
			}
		}
	}
	for _, i := range p.Imports {
		for _, bad := range coreDenyImports {
			if r.core && i == bad {
				out = append(out, rel+" importa "+i+" (domínio e casos de uso não fazem I/O)")
			}
		}
	}
	return out
}

func listPackages(t *testing.T) []pkg {
	t.Helper()
	cmd := exec.Command("go", "list", "-json=ImportPath,Imports,Deps", "./...")
	cmd.Dir = "../.."
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var pkgs []pkg
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for {
		var p pkg
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func TestDependencyRule(t *testing.T) {
	pkgs := listPackages(t)
	seen := map[string]bool{}
	for _, p := range pkgs {
		seen[strings.TrimPrefix(p.ImportPath, module)] = true
		for _, v := range violations(p) {
			t.Error(v)
		}
	}
	// pasta renomeada faria a regra passar sem checar nada
	for _, r := range rules {
		found := false
		for rel := range seen {
			found = found || matches(rel, r.pkg)
		}
		if !found {
			t.Errorf("regra de %s não casa com nenhum pacote", r.pkg)
		}
	}
}

// A regra pega o q tem q pegar: dependência indireta de pgx, rede no domínio, adapter
// conhecendo outro adapter, subpacote do domínio e pacote novo sem regra.
func TestDependencyRuleCatchesViolations(t *testing.T) {
	cases := []struct {
		p    pkg
		want string
	}{
		{pkg{ImportPath: module + "internal/app", Deps: []string{module + "internal/x", "github.com/jackc/pgx/v5"}}, "internal/app depende de internal/x"},
		{pkg{ImportPath: module + "internal/app", Deps: []string{"github.com/jackc/pgx/v5"}}, "internal/app depende de github.com/jackc/pgx/v5"},
		{pkg{ImportPath: module + "internal/domain/money", Deps: []string{"net/http"}}, "internal/domain/money depende de net/http"},
		{pkg{ImportPath: module + "internal/domain", Imports: []string{"os"}, Deps: []string{"os"}}, "internal/domain importa os"},
		{pkg{ImportPath: module + "internal/adapters/provider", Deps: []string{module + "internal/adapters/postgres"}}, "provider depende de internal/adapters/postgres"},
		{pkg{ImportPath: module + "internal/adapters/novo"}, "pacote sem regra"},
	}
	for _, c := range cases {
		got := strings.Join(violations(c.p), "; ")
		if !strings.Contains(got, c.want) {
			t.Errorf("%s: %q, esperado com %q", c.p.ImportPath, got, c.want)
		}
	}
	ok := pkg{ImportPath: module + "internal/adapters/postgres", Deps: []string{"fmt", "github.com/jackc/pgx/v5", module + "internal/app"}}
	if v := violations(ok); len(v) != 0 {
		t.Errorf("postgres legítimo acusado: %v", v)
	}
}
