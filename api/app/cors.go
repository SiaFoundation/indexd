package app

import (
	"net/http"
	"strings"

	"go.sia.tech/jape"
)

type route struct {
	last bool
	sub  map[string]route
}

const wildcardParamSegment = ":param"

// mergeOptionsSub merges src into dst
func mergeOptionsSub(dst, src map[string]route) {
	for key, val := range src {
		if existing, ok := dst[key]; ok {
			if val.last {
				existing.last = true
				dst[key] = existing
			}
			mergeOptionsSub(existing.sub, val.sub)
		} else {
			dst[key] = val
		}
	}
}

// addOptionsPath inserts an OPTIONS route path into the tree
// normalizing parameter segments to a single wildcard segment.
func addOptionsPath(m map[string]route, segments []string) {
	if len(segments) == 0 {
		return
	}
	seg := segments[0]
	if strings.HasPrefix(seg, ":") {
		seg = wildcardParamSegment
	}

	if seg != wildcardParamSegment {
		if wildcard, ok := m[wildcardParamSegment]; ok {
			// wildcard already exists at this level
			if len(segments) == 1 {
				wildcard.last = true
				m[wildcardParamSegment] = wildcard
			}
			addOptionsPath(wildcard.sub, segments[1:])
			return
		}
	}

	node, ok := m[seg]
	if !ok {
		node = route{sub: make(map[string]route)}
	}

	if seg == wildcardParamSegment {
		// merge all existing static routes at this
		// level into the wildcard route
		for key, r := range m {
			if key == wildcardParamSegment {
				continue
			}
			if r.last {
				node.last = true
			}
			mergeOptionsSub(node.sub, r.sub)
			delete(m, key)
		}
	}
	if len(segments) == 1 {
		node.last = true
	}
	m[seg] = node
	addOptionsPath(node.sub, segments[1:])
}

// flattenOptionsPaths collects all leaf paths from a resolved tree.
func flattenOptionsPaths(m map[string]route, prefix string) []string {
	var paths []string
	for seg, r := range m {
		path := prefix + "/" + seg
		if r.last {
			paths = append(paths, path)
		}
		paths = append(paths, flattenOptionsPaths(r.sub, path)...)
	}
	return paths
}

// optionsTreeContains returns true if a resolved tree has a leaf that
// covers the given path (exact match or wildcard).
func optionsTreeContains(m map[string]route, segments []string) bool {
	if len(segments) == 0 {
		return false
	}
	seg := segments[0]
	if strings.HasPrefix(seg, ":") {
		seg = wildcardParamSegment
	}

	check := func(node route) bool {
		if len(segments) == 1 {
			return node.last
		}
		return optionsTreeContains(node.sub, segments[1:])
	}

	if seg == wildcardParamSegment {
		for _, node := range m {
			if check(node) {
				return true
			}
		}
		return false
	}
	if node, ok := m[seg]; ok {
		return check(node)
	}
	if node, ok := m[wildcardParamSegment]; ok {
		return check(node)
	}
	return false
}

// corsMux is a helper that applies CORS middleware to the handlers in `enabledRoutes` and
// prevents OPTIONS handling in `disabledRoutes`.
//
// Cleaner than manually adding OPTIONS handlers to supported routes or doing string matching
// in a global middleware.
//
// If a route overlaps in both maps, it panics.
func corsMux(enabledRoutes map[string]jape.Handler, disabledRoutes map[string]jape.Handler) http.Handler {
	corsMiddleware := func(h jape.Handler) jape.Handler {
		return func(jc jape.Context) {
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Origin", "*")
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE")
			jc.ResponseWriter.Header().Set("Access-Control-Allow-Headers", "*")
			h(jc)
		}
	}

	optionsHandler := corsMiddleware(func(jc jape.Context) {
		jc.ResponseWriter.WriteHeader(http.StatusNoContent)
	})

	optionsTree := make(map[string]route)
	routes := make(map[string]jape.Handler, len(enabledRoutes)+len(disabledRoutes))
	for key, h := range enabledRoutes {
		// add CORS headers for this route
		routes[key] = corsMiddleware(h)
		// OPTIONs are collected separately since a parameterized route can overlap with a
		// static route (e.g. POST /slabs/prune and GET /slabs/:slabid)
		addOptionsPath(optionsTree, strings.Split(strings.Fields(key)[1], "/")[1:])
	}
	for key, h := range disabledRoutes {
		path := strings.Fields(key)[1]
		if optionsTreeContains(optionsTree, strings.Split(path, "/")[1:]) {
			panic("disabled route " + key + " conflicts with a CORS-enabled OPTIONS route")
		}
		routes[key] = h
	}
	for _, path := range flattenOptionsPaths(optionsTree, "") {
		// add explicit OPTIONS handlers for all enabled routes
		// so they will return the CORS headers too
		routes["OPTIONS "+path] = optionsHandler
	}

	return jape.Mux(routes)
}
