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
