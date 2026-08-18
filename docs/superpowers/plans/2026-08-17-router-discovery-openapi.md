# Router OpenAPI v3 + Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement real `/apis` (root discovery) and `/openapi/v3` (root
discovery index + per-group-version proxy) serving in `GrafanaRouter`,
replacing the current `501` stubs.

**Architecture:** Root `/apis` (`APIGroupList`) and root `/openapi/v3`
(discovery index) are synthesized once per `reconcile()` cycle straight from
each backend's `Manifest()` and stored as pre-marshaled `cachedDoc` (body +
RV-derived ETag) via `atomic.Pointer`, same single-writer/many-reader pattern
as the existing handler snapshot. The heavy per-group-version OpenAPI document
(`/openapi/v3/apis/<group>/<version>`) stays a pure reverse-proxy passthrough
to the owning backend, fronted by a `sync.Map` cache keyed by RV. All three
response types honor `If-None-Match` → `304`.

**Tech Stack:** Go stdlib only (`net/http`, `encoding/json`, `crypto/sha256`,
`sync`, `sync/atomic`) — no new dependencies. `httptest` for tests.

**Spec:** `docs/superpowers/specs/2026-08-17-router-discovery-openapi-design.md`

## Global Constraints

- **Stdlib only in `pkg/router`** — no
  `k8s.io/kube-openapi`, no `klog`. Discovery/OpenAPI JSON shapes are
  hand-rolled local structs with matching JSON tags (see spec Components).
- **No cross-group OpenAPI schema merge, ever.** Root `/openapi/v3` is a small
  path→hash index, not a merged document.
- **Cache-busting key is `RV`** everywhere (the same fingerprint
  `handlerEntry.lastRV` already tracks). No TTLs anywhere in this feature.
- **`Manifest()` may be nil defensively** — skip that backend (log + continue)
  when building discovery docs; never panic, never fail the whole reconcile.
- Every new/changed exported or package-level symbol must have a doc comment
  if the existing file already documents its siblings (match existing style
  in `router.go`/`backend.go`).

---

### Task 1: Local discovery/openapi JSON types + cachedDoc + ETag helper

**Files:**
- Create: `pkg/router/discovery.go`
- Test: `pkg/router/discovery_test.go`

**Interfaces:**
- Consumes: nothing (no dependency on other tasks).
- Produces:
  - `type cachedDoc struct { body []byte; etag string }`
  - `func quoteETag(s string) string` — wraps in `"..."` per RFC 7232.
  - `type apiGroupList struct{ Kind, APIVersion string; Groups []apiGroup }`
  - `type apiGroup struct{ Name string; Versions []groupVersionForDiscovery; PreferredVersion groupVersionForDiscovery }`
  - `type groupVersionForDiscovery struct{ GroupVersion, Version string }`
  - `type openAPIV3Discovery struct{ Paths map[string]openAPIV3DiscoveryGroupVersion }`
  - `type openAPIV3DiscoveryGroupVersion struct{ ServerRelativeURL string }`

- [ ] **Step 1: Write the failing test for `quoteETag`**

```go
// pkg/router/discovery_test.go
package router

import "testing"

func TestQuoteETag(t *testing.T) {
	got := quoteETag("abc123")
	want := `"abc123"`
	if got != want {
		t.Errorf("quoteETag(%q) = %q, want %q", "abc123", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/router/... -run TestQuoteETag -v`
Expected: FAIL — `undefined: quoteETag` (compile error, which counts as a
failing test here since the function doesn't exist yet).

- [ ] **Step 3: Write the types and helper**

```go
// pkg/router/discovery.go
package router

// cachedDoc is a pre-marshaled JSON response body plus its RV-derived ETag.
// Built once per reconcile cycle by buildAPIGroupList/buildOpenAPIV3Index and
// stored via atomic.Pointer for lock-free concurrent reads from the serving
// path.
type cachedDoc struct {
	body []byte
	etag string
}

// quoteETag wraps an opaque value as an HTTP entity tag per RFC 7232 §2.3.
func quoteETag(s string) string {
	return `"` + s + `"`
}

// Local mirrors of the k8s discovery/openapi JSON shapes (metav1.APIGroupList
// and kube-openapi's handler3.OpenAPIV3Discovery). Field names/tags match
// exactly so client-go/kubectl parse them unmodified. Defined locally instead
// of importing those packages because pkg/router is stdlib-only (see
// AGENTS.md: "No k8s apimachinery/klog deps") and handler3 isn't isolated —
// its file also imports klog, gnostic-models, uuid, goautoneg, and protobuf.

type apiGroupList struct {
	Kind       string     `json:"kind"`
	APIVersion string     `json:"apiVersion"`
	Groups     []apiGroup `json:"groups"`
}

type apiGroup struct {
	Name             string                     `json:"name"`
	Versions         []groupVersionForDiscovery `json:"versions"`
	PreferredVersion groupVersionForDiscovery   `json:"preferredVersion,omitempty"`
}

type groupVersionForDiscovery struct {
	GroupVersion string `json:"groupVersion"`
	Version      string `json:"version"`
}

type openAPIV3Discovery struct {
	Paths map[string]openAPIV3DiscoveryGroupVersion `json:"paths"`
}

type openAPIV3DiscoveryGroupVersion struct {
	ServerRelativeURL string `json:"serverRelativeURL"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/router/... -run TestQuoteETag -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/router/discovery.go pkg/router/discovery_test.go
git commit -m "router: add discovery/openapi JSON types and cachedDoc"
```

---

### Task 2: `buildAPIGroupList`

**Files:**
- Modify: `pkg/router/discovery.go`
- Test: `pkg/router/discovery_test.go`

**Interfaces:**
- Consumes: `Backend` (`pkg/router/types.go`: `RV() string`, `Group() string`,
  `Manifest() *app.ManifestData`), `cachedDoc`/`quoteETag`/`apiGroupList`/etc.
  from Task 1.
- Produces: `func buildAPIGroupList(backends []Backend) cachedDoc`

- [ ] **Step 1: Write the failing tests**

```go
// pkg/router/discovery_test.go — add:

import (
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-app-sdk/app"
)

// fakeBackend is a minimal Backend for discovery/cache tests. handler and
// proxyCalls are unused by discovery tests (zero value is fine); the
// per-group-version cache tests in a later task set them.
type fakeBackend struct {
	group    string
	rv       string
	manifest *app.ManifestData
	handler  http.Handler
}

func (b *fakeBackend) RV() string    { return b.rv }
func (b *fakeBackend) Group() string { return b.group }
func (b *fakeBackend) Manifest() *app.ManifestData { return b.manifest }
func (b *fakeBackend) Load(context.Context) (http.Handler, error) {
	if b.handler != nil {
		return b.handler, nil
	}
	return http.NotFoundHandler(), nil
}

func TestBuildAPIGroupList(t *testing.T) {
	backends := []Backend{
		&fakeBackend{group: "dashboard.grafana.app", rv: "10", manifest: &app.ManifestData{
			Group:            "dashboard.grafana.app",
			PreferredVersion: "v1alpha1",
			Versions: []app.ManifestVersion{
				{Name: "v0alpha1", Served: true},
				{Name: "v1alpha1", Served: true},
				{Name: "v2alpha1", Served: false}, // unserved: must be excluded
			},
		}},
		&fakeBackend{group: "folder.grafana.app", rv: "3", manifest: &app.ManifestData{
			Group:            "folder.grafana.app",
			PreferredVersion: "v0alpha1",
			Versions:         []app.ManifestVersion{{Name: "v0alpha1", Served: true}},
		}},
		// nil manifest must be skipped, not panic
		&fakeBackend{group: "broken.grafana.app", rv: "1", manifest: nil},
	}

	doc := buildAPIGroupList(backends)

	var list apiGroupList
	if err := json.Unmarshal(doc.body, &list); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if list.Kind != "APIGroupList" || list.APIVersion != "v1" {
		t.Errorf("got Kind=%q APIVersion=%q, want APIGroupList/v1", list.Kind, list.APIVersion)
	}
	if len(list.Groups) != 2 {
		t.Fatalf("got %d groups, want 2 (broken.grafana.app must be skipped): %+v", len(list.Groups), list.Groups)
	}
	// sorted alphabetically: dashboard before folder
	if list.Groups[0].Name != "dashboard.grafana.app" {
		t.Errorf("got Groups[0].Name=%q, want dashboard.grafana.app", list.Groups[0].Name)
	}
	if len(list.Groups[0].Versions) != 2 {
		t.Errorf("got %d served versions for dashboard.grafana.app, want 2 (v2alpha1 unserved excluded): %+v",
			len(list.Groups[0].Versions), list.Groups[0].Versions)
	}
	wantPreferred := groupVersionForDiscovery{GroupVersion: "dashboard.grafana.app/v1alpha1", Version: "v1alpha1"}
	if list.Groups[0].PreferredVersion != wantPreferred {
		t.Errorf("got PreferredVersion=%+v, want %+v", list.Groups[0].PreferredVersion, wantPreferred)
	}
	if doc.etag == "" {
		t.Error("etag must not be empty")
	}
}

func TestBuildAPIGroupListETagChangesWithRV(t *testing.T) {
	mk := func(rv string) []Backend {
		return []Backend{&fakeBackend{group: "dashboard.grafana.app", rv: rv, manifest: &app.ManifestData{
			Group:    "dashboard.grafana.app",
			Versions: []app.ManifestVersion{{Name: "v1alpha1", Served: true}},
		}}}
	}
	a := buildAPIGroupList(mk("1"))
	b := buildAPIGroupList(mk("2"))
	if a.etag == b.etag {
		t.Errorf("etag did not change when RV changed: both %q", a.etag)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/router/... -run TestBuildAPIGroupList -v`
Expected: FAIL — `undefined: buildAPIGroupList`

- [ ] **Step 3: Write the implementation**

```go
// pkg/router/discovery.go — add:

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

// buildAPIGroupList synthesizes the /apis root document (APIGroupList) from
// each backend's Manifest — Group, served Versions, PreferredVersion. No
// backend round-trip: this is pure local synthesis, called once per
// reconcile cycle alongside the handler snapshot.
func buildAPIGroupList(backends []Backend) cachedDoc {
	sorted := sortedManifestBackends(backends, "APIGroupList")

	groups := make([]apiGroup, 0, len(sorted))
	var hashInput strings.Builder
	for _, b := range sorted {
		m := b.Manifest()
		versions := make([]groupVersionForDiscovery, 0, len(m.Versions))
		for _, v := range m.Versions {
			if !v.Served {
				continue
			}
			versions = append(versions, groupVersionForDiscovery{
				GroupVersion: m.Group + "/" + v.Name,
				Version:      v.Name,
			})
		}
		var preferred groupVersionForDiscovery
		if m.PreferredVersion != "" {
			preferred = groupVersionForDiscovery{
				GroupVersion: m.Group + "/" + m.PreferredVersion,
				Version:      m.PreferredVersion,
			}
		}
		groups = append(groups, apiGroup{
			Name:             m.Group,
			Versions:         versions,
			PreferredVersion: preferred,
		})
		fmt.Fprintf(&hashInput, "%s=%s;", b.Group(), b.RV())
	}

	list := apiGroupList{Kind: "APIGroupList", APIVersion: "v1", Groups: groups}
	body, err := json.Marshal(list)
	if err != nil {
		// list is a fixed, well-typed struct: Marshal cannot fail in practice.
		// Fall back to an empty-but-valid document rather than serving garbage.
		slog.Error("router: failed to marshal APIGroupList", "error", err)
		body = []byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
	}
	return cachedDoc{body: body, etag: quoteETag(hashHex(hashInput.String()))}
}

// sortedManifestBackends filters out backends with a nil Manifest (logged and
// skipped, never fatal — matches the existing duplicate-group
// warn-and-continue tolerance for bad GitOps config) and returns the rest
// sorted by group name for deterministic output and a stable hash input.
func sortedManifestBackends(backends []Backend, forDoc string) []Backend {
	out := make([]Backend, 0, len(backends))
	for _, b := range backends {
		if b.Manifest() == nil {
			slog.Warn("router: skipping backend with nil manifest", "group", b.Group(), "doc", forDoc)
			continue
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group() < out[j].Group() })
	return out
}

// hashHex returns a short hex digest of s, used to build a cachedDoc's ETag
// from the sorted group/RV pairs that went into it.
func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/router/... -run TestBuildAPIGroupList -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/router/discovery.go pkg/router/discovery_test.go
git commit -m "router: synthesize APIGroupList from backend manifests"
```

---

### Task 3: `buildOpenAPIV3Index`

**Files:**
- Modify: `pkg/router/discovery.go`
- Test: `pkg/router/discovery_test.go`

**Interfaces:**
- Consumes: `sortedManifestBackends`, `hashHex`, `quoteETag`, `cachedDoc`,
  `openAPIV3Discovery`/`openAPIV3DiscoveryGroupVersion` from Tasks 1-2.
- Produces: `func buildOpenAPIV3Index(backends []Backend) cachedDoc`

- [ ] **Step 1: Write the failing test**

```go
// pkg/router/discovery_test.go — add:

func TestBuildOpenAPIV3Index(t *testing.T) {
	backends := []Backend{
		&fakeBackend{group: "dashboard.grafana.app", rv: "10", manifest: &app.ManifestData{
			Group: "dashboard.grafana.app",
			Versions: []app.ManifestVersion{
				{Name: "v0alpha1", Served: true},
				{Name: "v1alpha1", Served: false}, // unserved: excluded
			},
		}},
		&fakeBackend{group: "broken.grafana.app", rv: "1", manifest: nil},
	}

	doc := buildOpenAPIV3Index(backends)

	var idx openAPIV3Discovery
	if err := json.Unmarshal(doc.body, &idx); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(idx.Paths) != 1 {
		t.Fatalf("got %d paths, want 1: %+v", len(idx.Paths), idx.Paths)
	}
	entry, ok := idx.Paths["apis/dashboard.grafana.app/v0alpha1"]
	if !ok {
		t.Fatalf("missing path apis/dashboard.grafana.app/v0alpha1, got %+v", idx.Paths)
	}
	want := "/openapi/v3/apis/dashboard.grafana.app/v0alpha1?hash=10"
	if entry.ServerRelativeURL != want {
		t.Errorf("got ServerRelativeURL=%q, want %q", entry.ServerRelativeURL, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/router/... -run TestBuildOpenAPIV3Index -v`
Expected: FAIL — `undefined: buildOpenAPIV3Index`

- [ ] **Step 3: Write the implementation**

```go
// pkg/router/discovery.go — add:

// buildOpenAPIV3Index synthesizes the /openapi/v3 root document: a small
// path -> {serverRelativeURL} map (never a merged schema — see AGENTS.md
// "Discovery endpoints" / the design spec's "no cross-group merge" decision).
// One entry per served group/version, hash-busted by that group's RV.
func buildOpenAPIV3Index(backends []Backend) cachedDoc {
	sorted := sortedManifestBackends(backends, "OpenAPIV3Discovery")

	paths := make(map[string]openAPIV3DiscoveryGroupVersion, len(sorted))
	var hashInput strings.Builder
	for _, b := range sorted {
		m := b.Manifest()
		for _, v := range m.Versions {
			if !v.Served {
				continue
			}
			key := fmt.Sprintf("apis/%s/%s", m.Group, v.Name)
			paths[key] = openAPIV3DiscoveryGroupVersion{
				ServerRelativeURL: fmt.Sprintf("/openapi/v3/%s?hash=%s", key, b.RV()),
			}
			fmt.Fprintf(&hashInput, "%s=%s;", key, b.RV())
		}
	}

	doc := openAPIV3Discovery{Paths: paths}
	body, err := json.Marshal(doc)
	if err != nil {
		slog.Error("router: failed to marshal OpenAPIV3Discovery", "error", err)
		body = []byte(`{"paths":{}}`)
	}
	return cachedDoc{body: body, etag: quoteETag(hashHex(hashInput.String()))}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/router/... -run TestBuildOpenAPIV3Index -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/router/discovery.go pkg/router/discovery_test.go
git commit -m "router: synthesize OpenAPI v3 discovery index from backend manifests"
```

---

### Task 4: Wire root docs into `reconcile`/`publish`, serve them with ETag/304

**Files:**
- Modify: `pkg/router/router.go`
- Modify: `pkg/router/router_test.go` (fix the two now-stale `501` assertions)

**Interfaces:**
- Consumes: `cachedDoc`, `buildAPIGroupList`, `buildOpenAPIV3Index` from Tasks
  1-3.
- Produces:
  - `GrafanaRouter.apiGroupList atomic.Pointer[cachedDoc]`
  - `GrafanaRouter.openapiIndex atomic.Pointer[cachedDoc]`
  - `func serveCachedDoc(w http.ResponseWriter, req *http.Request, doc *cachedDoc)`
    — used by Task 6 too.
  - `publish(backends []Backend)` (signature changed from `publish()`).

- [ ] **Step 1: Write the failing tests**

Replace the two placeholder-501 lines in the existing `TestHandleFuncRoutesByGroup`
table (`pkg/router/router_test.go:58-59`) — they currently assert `501`, which
becomes wrong the moment root docs are real. Also add a dedicated ETag/304 test.

```go
// pkg/router/router_test.go

// In TestHandleFuncRoutesByGroup's cases slice, replace these two entries:
//   {"/openapi/v3", "", http.StatusNotImplemented},
//   {"/openapi/v3/apis/dashboard.grafana.app/v1alpha1", "", http.StatusNotImplemented},
// with:
		{"/openapi/v3", "", http.StatusOK}, // now router-synthesized; body checked separately below

// The second removed line (the per-group-version path) moves to Task 6,
// which needs real backend wiring to test proxy+cache behavior — leave it out
// of this table entirely; withGroups' fake handlers don't serve real OpenAPI
// bytes, so there's nothing meaningful to assert on that path here.

// New test:
func TestServeRootDocsWithETag(t *testing.T) {
	s := withGroups("dashboard.grafana.app")
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.HandleFunc(w, req, next)
	})

	for _, path := range []string{"/apis", "/openapi/v3"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("path %q: got code %d, want 200", path, rec.Code)
		}
		etag := rec.Header().Get("ETag")
		if etag == "" {
			t.Fatalf("path %q: missing ETag header", path)
		}

		// Conditional GET with the returned ETag must 304 with no body.
		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, path, nil)
		req2.Header.Set("If-None-Match", etag)
		h.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusNotModified {
			t.Errorf("path %q: got code %d with matching If-None-Match, want 304", path, rec2.Code)
		}
		if rec2.Body.Len() != 0 {
			t.Errorf("path %q: 304 response had a body: %q", path, rec2.Body.String())
		}
	}
}
```

Note: `withGroups` (existing test helper in `router_test.go`) seeds
`s.entries` directly and calls `s.publish()` — this task changes `publish`'s
signature to `publish(backends []Backend)`, so `withGroups` must pass `nil`
(no manifests seeded ⇒ `apiGroupList`/`openapiIndex` synthesize from zero
backends, which is fine: valid empty `APIGroupList`/`OpenAPIV3Discovery`, not
an error). Update it:

```go
// pkg/router/router_test.go — withGroups, change the last line:
	s.publish(nil)
	return s
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/router/... -v`
Expected: FAIL to compile (`publish` signature mismatch) and/or `TestServeRootDocsWithETag`
fails with 501s, until Step 3 lands.

- [ ] **Step 3: Implement**

```go
// pkg/router/router.go

// Add to GrafanaRouter struct (after snapshot field):
	// apiGroupList and openapiIndex are the router-synthesized root documents
	// for /apis and /openapi/v3, rebuilt from backends' Manifest() on every
	// reconcile and stored atomically alongside snapshot. Never a
	// cross-group OpenAPI schema merge — see AGENTS.md / the design spec.
	apiGroupList atomic.Pointer[cachedDoc]
	openapiIndex atomic.Pointer[cachedDoc]
```

```go
// NewGrafanaRouter: seed both with valid empty documents so Ready-before-first-reconcile
// requests to /apis or /openapi/v3 get a well-formed empty doc, not a nil-pointer panic.
func NewGrafanaRouter(loader RoutesLoader) *GrafanaRouter {
	r := &GrafanaRouter{
		loader:  loader,
		entries: map[string]*handlerEntry{},
	}
	empty := map[string]http.Handler{}
	r.snapshot.Store(&empty)
	emptyGroups := buildAPIGroupList(nil)
	r.apiGroupList.Store(&emptyGroups)
	emptyIndex := buildOpenAPIV3Index(nil)
	r.openapiIndex.Store(&emptyIndex)
	return r
}
```

```go
// publish: signature changes from publish() to publish(backends []Backend);
// still called only from reconcile, still the sole writer of these atomics.
func (r *GrafanaRouter) publish(backends []Backend) {
	snap := make(map[string]http.Handler, len(r.entries))
	for group, e := range r.entries {
		snap[group] = e.handler
	}
	r.snapshot.Store(&snap)

	groupList := buildAPIGroupList(backends)
	r.apiGroupList.Store(&groupList)

	index := buildOpenAPIV3Index(backends)
	r.openapiIndex.Store(&index)
}
```

```go
// reconcile: change the call site (was r.publish()):
	r.publish(backends)
```

```go
// serveAPIGroupList: replace the 501 stub.
func (cr *GrafanaRouter) serveAPIGroupList(w http.ResponseWriter, req *http.Request) {
	serveCachedDoc(w, req, cr.apiGroupList.Load())
}
```

```go
// serveCachedDoc writes a synthesized document, honoring conditional GET via
// If-None-Match against the document's RV-derived ETag. Shared by
// serveAPIGroupList and the /openapi/v3 root doc (Task 5).
func serveCachedDoc(w http.ResponseWriter, req *http.Request, doc *cachedDoc) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", doc.etag)
	if req.Header.Get("If-None-Match") == doc.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(doc.body)
}
```

Leave `serveOpenAPIV3` as the existing 501 stub for now (Task 5 replaces its
root-path branch; Task 6 adds the per-group-version branch) — but its root
path is exercised by `TestServeRootDocsWithETag` above via `/openapi/v3`, so
Task 5 must land before that test can pass. **Sequencing note:** write Task 4
and Task 5 in the same working session if running non-interactively; Task 4's
new test intentionally exercises both `/apis` and `/openapi/v3` so it also
verifies Task 5's root-doc branch once written. If running task-by-task with
review gates, it's fine for `TestServeRootDocsWithETag`'s `/openapi/v3` case
to fail after Task 4 alone and pass after Task 5 — call this out in the Task 4
PR/review note so the reviewer doesn't mistake it for a regression.

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/router/... -v`
Expected: `TestHandleFuncRoutesByGroup`, `TestHandleFuncRootDiscoveryNotProxied`
PASS. `TestServeRootDocsWithETag`'s `/apis` case PASSES; its `/openapi/v3` case
still fails (501) until Task 5 — expected, not a regression (see note above).

- [ ] **Step 5: Commit**

```bash
git add pkg/router/router.go pkg/router/router_test.go
git commit -m "router: synthesize and serve /apis root from manifests with ETag/304"
```

---

### Task 5: Root `/openapi/v3` doc + path parsing/dispatch

**Files:**
- Modify: `pkg/router/router.go`
- Modify: `pkg/router/router_test.go`

**Interfaces:**
- Consumes: `serveCachedDoc`, `cr.openapiIndex` from Task 4.
- Produces:
  - `func parseOpenAPIGroupVersionPath(path string) (group, version string, ok bool)`
  - `serveOpenAPIV3` signature changes to
    `func (cr *GrafanaRouter) serveOpenAPIV3(w http.ResponseWriter, req *http.Request, next http.Handler)`
    (needs `next` so malformed/unknown-group subpaths can fall through — Task 6
    fills in the group-known branch; this task makes the "not a recognized
    shape" and "root" branches real, and leaves group-version dispatch as a
    `next.ServeHTTP` fallthrough for now, since no backend lookup/cache exists
    until Task 6).

- [ ] **Step 1: Write the failing tests**

```go
// pkg/router/router_test.go — add:

func TestParseOpenAPIGroupVersionPath(t *testing.T) {
	cases := []struct {
		path        string
		wantGroup   string
		wantVersion string
		wantOK      bool
	}{
		{"/openapi/v3/apis/dashboard.grafana.app/v1alpha1", "dashboard.grafana.app", "v1alpha1", true},
		{"/openapi/v3", "", "", false},                          // root doc, not a group/version path
		{"/openapi/v3/", "", "", false},
		{"/openapi/v3/apis/dashboard.grafana.app", "", "", false}, // missing version
		{"/openapi/v3/apis/dashboard.grafana.app/v1alpha1/extra", "", "", false}, // too many segments
		{"/openapi/v3/api/v1", "", "", false},                    // core-style "api/", not supported
	}
	for _, tc := range cases {
		group, version, ok := parseOpenAPIGroupVersionPath(tc.path)
		if ok != tc.wantOK || group != tc.wantGroup || version != tc.wantVersion {
			t.Errorf("parseOpenAPIGroupVersionPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.path, group, version, ok, tc.wantGroup, tc.wantVersion, tc.wantOK)
		}
	}
}

// TestOpenAPIV3RootFallsThroughForMalformedSubpath: a subpath that doesn't
// parse as apis/<group>/<version> isn't ours; it must fall through to next,
// same primacy rule as an unknown /apis group.
func TestOpenAPIV3MalformedSubpathFallsThrough(t *testing.T) {
	s := withGroups("dashboard.grafana.app")
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s.HandleFunc(w, req, next)
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/openapi/v3/apis/dashboard.grafana.app", nil))
	if rec.Code != http.StatusTeapot {
		t.Errorf("got code %d, want 418 (fell through to next)", rec.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/router/... -run 'TestParseOpenAPIGroupVersionPath|TestOpenAPIV3MalformedSubpathFallsThrough|TestServeRootDocsWithETag' -v`
Expected: FAIL — `undefined: parseOpenAPIGroupVersionPath`; `TestServeRootDocsWithETag`'s
`/openapi/v3` case still 501.

- [ ] **Step 3: Implement**

```go
// pkg/router/router.go

// parseOpenAPIGroupVersionPath extracts group and version from a path of the
// exact shape "/openapi/v3/apis/<group>/<version>". ok is false for the root
// "/openapi/v3" doc itself, a trailing slash, a missing version, extra
// segments, or the k8s "api/<version>" core-group shape (not applicable here
// — this router has no core group).
func parseOpenAPIGroupVersionPath(path string) (group, version string, ok bool) {
	rest, hasPrefix := strings.CutPrefix(path, openapiV3Prefix+"/")
	if !hasPrefix || rest == "" {
		return "", "", false
	}
	rest, hasAPIs := strings.CutPrefix(rest, "apis/")
	if !hasAPIs || rest == "" {
		return "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
```

```go
// serveOpenAPIV3: replace the 501 stub. Root doc is real; group/version
// dispatch falls through to next until Task 6 adds the proxy+cache.
func (cr *GrafanaRouter) serveOpenAPIV3(w http.ResponseWriter, req *http.Request, next http.Handler) {
	if req.URL.Path == openapiV3Prefix {
		serveCachedDoc(w, req, cr.openapiIndex.Load())
		return
	}
	_, _, ok := parseOpenAPIGroupVersionPath(req.URL.Path)
	if !ok {
		next.ServeHTTP(w, req)
		return
	}
	// TODO(Task 6): look up the owning backend by group, serve from the
	// RV-keyed cache or proxy through.
	next.ServeHTTP(w, req)
}
```

```go
// HandleFunc: update the call site to pass next.
// Before:
//   if path == openapiV3Prefix || strings.HasPrefix(path, openapiV3Prefix+"/") {
//       cr.serveOpenAPIV3(w, req)
//       return
//   }
// After:
	if path == openapiV3Prefix || strings.HasPrefix(path, openapiV3Prefix+"/") {
		cr.serveOpenAPIV3(w, req, next)
		return
	}
```

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/router/... -v`
Expected: all PASS, including `TestServeRootDocsWithETag`'s `/openapi/v3` case now.

- [ ] **Step 5: Commit**

```bash
git add pkg/router/router.go pkg/router/router_test.go
git commit -m "router: serve /openapi/v3 root doc, parse group/version subpaths"
```

---

### Task 6: Per-group-version OpenAPI proxy with RV-keyed cache

**Files:**
- Modify: `pkg/router/router.go`
- Create: `pkg/router/openapi_cache.go`
- Create: `pkg/router/openapi_cache_test.go`
- Modify: `pkg/router/router_test.go` (`withGroups` needs real per-group RV
  visible at serve time — see Interfaces below)

**Interfaces:**
- Consumes: `parseOpenAPIGroupVersionPath`, `serveOpenAPIV3` from Task 5;
  `handlerEntry`, `entries`, `snapshot`, `publish` from existing code / Task 4.
- Produces:
  - `type servingEntry struct { handler http.Handler; rv string }` — snapshot's
    value type changes from `http.Handler` to `servingEntry` (breaking change
    to an internal type; both existing use sites — `publish` and `HandleFunc`'s
    group dispatch — are updated in this task).
  - `type openapiCacheEntry struct { rv, etag string; body []byte }`
  - `GrafanaRouter.openapiDocs sync.Map` (key `"group/version"` → `openapiCacheEntry`)
  - `func (cr *GrafanaRouter) serveOpenAPIGroupVersion(w http.ResponseWriter, req *http.Request, next http.Handler, group, version string)`
  - `func stripConditionalHeaders(req *http.Request)`

**Why `snapshot`'s value type must change:** the cache needs the *current* RV
for a group at serve time to know whether a cached entry is stale. `lastRV`
today lives only on `handlerEntry`, which belongs to `entries` — owned
exclusively by the reconcile goroutine, unsafe to read concurrently from
serving goroutines. `snapshot` is the one structure already safe for
concurrent reads (`atomic.Pointer`, rebuilt wholesale on every `publish`), so
RV moves there too.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/router/openapi_cache_test.go
package router

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// countingHandler serves body and counts how many times it was hit, so tests
// can assert the cache actually avoided a re-fetch.
type countingHandler struct {
	body string
	hits atomic.Int64
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.hits.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(h.body))
}

// buildRouterWithBackend seeds a router with one real handlerEntry (fake
// upstream handler + given rv) via publish, so snapshot carries a real RV —
// unlike withGroups' fixed lastRV:"1", these tests need to bump RV mid-test.
func buildRouterWithBackend(group, rv string, upstream http.Handler) *GrafanaRouter {
	s := NewGrafanaRouter(stubLoader{})
	s.entries[group] = &handlerEntry{handler: upstream, lastRV: rv}
	s.publish(nil)
	return s
}

func TestOpenAPIGroupVersionCachesUntilRVChanges(t *testing.T) {
	upstream := &countingHandler{body: `{"openapi":"3.0.0"}`}
	s := buildRouterWithBackend("dashboard.grafana.app", "5", upstream)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { s.HandleFunc(w, req, next) })

	path := "/openapi/v3/apis/dashboard.grafana.app/v1alpha1"

	// First request: cache miss, proxies through.
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, path, nil))
	if rec1.Code != http.StatusOK || rec1.Body.String() != upstream.body {
		t.Fatalf("first request: got code=%d body=%q, want 200 %q", rec1.Code, rec1.Body.String(), upstream.body)
	}
	if got := upstream.hits.Load(); got != 1 {
		t.Fatalf("after first request, upstream hits = %d, want 1", got)
	}

	// Second request, same RV: served from cache, no new upstream hit.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, path, nil))
	if rec2.Code != http.StatusOK || rec2.Body.String() != upstream.body {
		t.Fatalf("second request: got code=%d body=%q, want 200 %q", rec2.Code, rec2.Body.String(), upstream.body)
	}
	if got := upstream.hits.Load(); got != 1 {
		t.Fatalf("after second request, upstream hits = %d, want still 1 (cache hit)", got)
	}

	// Bump RV (simulates reconcile picking up a manifest change) and re-request:
	// cache must be treated as stale, upstream hit again.
	s.entries["dashboard.grafana.app"] = &handlerEntry{handler: upstream, lastRV: "6"}
	s.publish(nil)
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, path, nil))
	if rec3.Code != http.StatusOK {
		t.Fatalf("third request: got code=%d, want 200", rec3.Code)
	}
	if got := upstream.hits.Load(); got != 2 {
		t.Fatalf("after RV bump, upstream hits = %d, want 2 (cache invalidated)", got)
	}
}

func TestOpenAPIGroupVersionIfNoneMatch304(t *testing.T) {
	upstream := &countingHandler{body: `{"openapi":"3.0.0"}`}
	s := buildRouterWithBackend("dashboard.grafana.app", "5", upstream)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { s.HandleFunc(w, req, next) })
	path := "/openapi/v3/apis/dashboard.grafana.app/v1alpha1"

	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, path, nil))
	etag := rec1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag on first response")
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2.Header.Set("If-None-Match", etag)
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotModified {
		t.Errorf("got code %d, want 304", rec2.Code)
	}
	if rec2.Body.Len() != 0 {
		t.Errorf("304 response had a body: %q", rec2.Body.String())
	}
	if got := upstream.hits.Load(); got != 1 {
		t.Errorf("upstream hits = %d, want 1 (304 must not re-hit upstream)", got)
	}
}

func TestOpenAPIGroupVersionUnknownGroupFallsThrough(t *testing.T) {
	s := buildRouterWithBackend("dashboard.grafana.app", "5", &countingHandler{body: "{}"})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { s.HandleFunc(w, req, next) })
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/openapi/v3/apis/unknown.grafana.app/v1", nil))
	if rec.Code != http.StatusTeapot {
		t.Errorf("got code %d, want 418 (fell through)", rec.Code)
	}
}

// TestOpenAPIGroupVersionStripsConditionalHeaders: the upstream fake honors
// (unstripped) If-None-Match with its own unrelated 304 — proving the router
// strips conditional headers before proxying on a cache miss, so it always
// gets a real body to judge and cache. Regression test for the phantom-304
// bug the design spec calls out explicitly.
type conditionalUpstream struct {
	body string
}

func (u *conditionalUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("If-None-Match") != "" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(u.body))
}

func TestOpenAPIGroupVersionStripsConditionalHeaders(t *testing.T) {
	upstream := &conditionalUpstream{body: `{"openapi":"3.0.0"}`}
	s := buildRouterWithBackend("dashboard.grafana.app", "5", upstream)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { s.HandleFunc(w, req, next) })

	req := httptest.NewRequest(http.MethodGet, "/openapi/v3/apis/dashboard.grafana.app/v1alpha1", nil)
	// A stale/foreign If-None-Match that does NOT match our current RV-based
	// ETag, so the router proceeds to proxy — the case that must strip it.
	req.Header.Set("If-None-Match", `"some-other-etag"`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("got code %d, want 200 (upstream must not see the forwarded If-None-Match and phantom-304)", rec.Code)
	}
	if rec.Body.String() != upstream.body {
		t.Errorf("got body %q, want %q", rec.Body.String(), upstream.body)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/router/... -v`
Expected: FAIL to compile — `s.entries[group] = &handlerEntry{...}` compiles
fine (unchanged), but `serveOpenAPIGroupVersion`/`openapiDocs`/`servingEntry`
don't exist yet, and today's `next.ServeHTTP` fallthrough in `serveOpenAPIV3`
means `TestOpenAPIGroupVersionCachesUntilRVChanges` currently gets 418, not 200.

- [ ] **Step 3: Implement**

```go
// pkg/router/openapi_cache.go
package router

import (
	"bytes"
	"net/http"
)

// openapiCacheEntry is one cached per-group-version OpenAPI v3 document.
// Valid only while rv matches the backend's current RV (checked by the
// caller); a stale rv is a cache miss, not an eviction — the sync.Map entry
// is simply overwritten on the next successful fetch.
type openapiCacheEntry struct {
	rv   string
	etag string
	body []byte
}

// stripConditionalHeaders removes conditional-GET headers from a request
// before proxying it upstream on a cache miss. Without this, a client's
// If-None-Match that didn't match our RV-based ETag (so we decided to proxy)
// could still coincidentally match the backend's own unrelated ETag scheme,
// producing a bodyless 304 we'd have no way to distinguish from "unchanged"
// — a phantom empty response with nothing to cache or serve. Stripping
// guarantees the backend always gives us a real, judgeable status code.
func stripConditionalHeaders(req *http.Request) {
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
}

// captureWriter records a proxied response (status + body) so it can be
// cached on success before being relayed to the real client, without letting
// the backend write directly to the real ResponseWriter first.
type captureWriter struct {
	header     http.Header
	statusCode int
	body       bytes.Buffer
}

func newCaptureWriter() *captureWriter {
	return &captureWriter{header: make(http.Header), statusCode: http.StatusOK}
}

func (c *captureWriter) Header() http.Header { return c.header }
func (c *captureWriter) Write(p []byte) (int, error) { return c.body.Write(p) }
func (c *captureWriter) WriteHeader(code int)        { c.statusCode = code }
```

```go
// pkg/router/router.go

// Add near the top of the file (with the other type defs, before handlerEntry):

// servingEntry is the immutable per-group record published into snapshot: the
// proxy handler plus the RV. RV is needed at serve time to validate/label the
// per-group-version openapi cache; entries (reconcile-goroutine-owned) isn't
// safe to read from serving goroutines, so RV is duplicated here.
type servingEntry struct {
	handler http.Handler
	rv      string
}
```

```go
// GrafanaRouter struct: change snapshot's type and add openapiDocs.
	// snapshot is the immutable group -> servingEntry map used to serve
	// requests. reconcile rebuilds and atomically stores it; serving loads it.
	snapshot atomic.Pointer[map[string]servingEntry]

	// openapiDocs caches per-group-version OpenAPI v3 documents fetched from
	// the owning backend, keyed by "group/version". Written by many
	// concurrent serving goroutines on cache-miss (unlike snapshot/
	// apiGroupList/openapiIndex, which have exactly one writer, reconcile),
	// so it's a sync.Map rather than an atomic.Pointer swap. A stale rv is
	// simply overwritten on next fetch, not actively evicted.
	openapiDocs sync.Map
```

```go
// NewGrafanaRouter: update snapshot's zero value type.
	empty := map[string]servingEntry{}
	r.snapshot.Store(&empty)
```

```go
// publish: populate rv alongside handler.
func (r *GrafanaRouter) publish(backends []Backend) {
	snap := make(map[string]servingEntry, len(r.entries))
	for group, e := range r.entries {
		snap[group] = servingEntry{handler: e.handler, rv: e.lastRV}
	}
	r.snapshot.Store(&snap)

	groupList := buildAPIGroupList(backends)
	r.apiGroupList.Store(&groupList)

	index := buildOpenAPIV3Index(backends)
	r.openapiIndex.Store(&index)
}
```

```go
// HandleFunc: update the group-dispatch block for snapshot's new value type.
// Before:
//   handlers := *cr.snapshot.Load()
//   h, ok := handlers[group]
//   ...
//   h.ServeHTTP(w, req)
// After:
	handlers := *cr.snapshot.Load()
	entry, ok := handlers[group]
	if !ok {
		next.ServeHTTP(w, req)
		return
	}
	entry.handler.ServeHTTP(w, req)
```

```go
// serveOpenAPIV3: fill in the group/version branch left as next.ServeHTTP
// TODO in Task 5.
func (cr *GrafanaRouter) serveOpenAPIV3(w http.ResponseWriter, req *http.Request, next http.Handler) {
	if req.URL.Path == openapiV3Prefix {
		serveCachedDoc(w, req, cr.openapiIndex.Load())
		return
	}
	group, version, ok := parseOpenAPIGroupVersionPath(req.URL.Path)
	if !ok {
		next.ServeHTTP(w, req)
		return
	}
	cr.serveOpenAPIGroupVersion(w, req, next, group, version)
}

// serveOpenAPIGroupVersion serves one group's OpenAPI v3 document: a
// conditional-GET-aware, RV-keyed cache in front of a plain proxy to the
// owning backend. Never merges across groups — this is one backend's
// document, verbatim.
func (cr *GrafanaRouter) serveOpenAPIGroupVersion(w http.ResponseWriter, req *http.Request, next http.Handler, group, version string) {
	handlers := *cr.snapshot.Load()
	entry, ok := handlers[group]
	if !ok {
		next.ServeHTTP(w, req)
		return
	}

	etag := quoteETag(entry.rv)
	if req.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	cacheKey := group + "/" + version
	if cached, ok := cr.openapiDocs.Load(cacheKey); ok {
		c := cached.(openapiCacheEntry)
		if c.rv == entry.rv {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("ETag", c.etag)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(c.body)
			return
		}
	}

	// Cache miss or stale rv: proxy through, capturing the response so it can
	// be cached on success. Strip conditional headers first — see
	// stripConditionalHeaders' doc comment for why.
	proxyReq := req.Clone(req.Context())
	stripConditionalHeaders(proxyReq)
	rec := newCaptureWriter()
	entry.handler.ServeHTTP(rec, proxyReq)

	for k, v := range rec.header {
		w.Header()[k] = v
	}
	if rec.statusCode == http.StatusOK {
		cr.openapiDocs.Store(cacheKey, openapiCacheEntry{rv: entry.rv, etag: etag, body: rec.body.Bytes()})
		w.Header().Set("ETag", etag)
	}
	w.WriteHeader(rec.statusCode)
	_, _ = w.Write(rec.body.Bytes())
}
```

Add `"sync"` to `router.go`'s import block (for `sync.Map`) — `sync/atomic` is
already imported.

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/router/... -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/router/router.go pkg/router/openapi_cache.go pkg/router/openapi_cache_test.go pkg/router/router_test.go
git commit -m "router: proxy+cache per-group-version OpenAPI v3 docs, keyed by RV"
```

---

### Task 7: Full-package verification

**Files:** none changed — verification only.

- [ ] **Step 1: Run the full package test suite**

Run: `go test ./pkg/router/... -v -race`
Expected: all tests PASS, including under `-race` (the `sync.Map` cache and
`atomic.Pointer` fields are the concurrency-sensitive surface here).

- [ ] **Step 2: Run go vet and the repo linter on the package**

Run: `go vet ./pkg/router/...`
Expected: no output (clean).

Run: `golangci-lint run ./pkg/router/...` (or `make lint-go` if scoping to one
package isn't convenient)
Expected: no new lint findings.

- [ ] **Step 3: Confirm no new dependencies were introduced**

Run: `git diff main -- go.mod go.sum`
Expected: empty — this feature must not touch `go.mod`/`go.sum` at all
(stdlib-only constraint from `pkg/router/AGENTS.md`).

- [ ] **Step 4: Semgrep scan (org security policy)**

Run the project's semgrep MCP tool (or `semgrep scan pkg/router/`) over the
changed files per the standing org instruction to scan generated/written code
for security issues before landing. Expected: no findings on the new proxy
capture/header-stripping code (the sensitive surface per `pkg/router/AGENTS.md`'s
own Security section).

- [ ] **Step 5: Final commit (docs cross-reference only, if anything changed)**

If any of the above steps required a fix, commit it with a message describing
the fix. If Steps 1-4 were clean, there is nothing to commit for this task.
