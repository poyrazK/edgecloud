package domain

import "github.com/lib/pq"

// StringArrayFrom converts a []string to a pq.StringArray for DB writes.
// nil in -> empty array out (so the NOT NULL DEFAULT '{}' column never
// sees a SQL NULL). Useful at the service/repo boundary where a domain
// type is being constructed from a []string parameter.
func StringArrayFrom(s []string) pq.StringArray {
	if s == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(s)
}

// StringArrayTo returns the underlying []string of a pq.StringArray for
// wire formats that want a plain slice. nil in -> nil out (preserves
// the nil-vs-empty distinction across the boundary, so JSON encoding
// stays consistent with the source field).
func StringArrayTo(a pq.StringArray) []string {
	if a == nil {
		return nil
	}
	return []string(a)
}
