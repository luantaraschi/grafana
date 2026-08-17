package router

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

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
