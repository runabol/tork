package fts

import (
	"regexp"
	"strings"
)

var searchQueryRE = regexp.MustCompile(`[^a-zA-Z0-9@.\-\s]+`)

func PrepareQuery(q string) string {
	q = searchQueryRE.ReplaceAllString(q, "")
	tokens := strings.Fields(q)
	q = ""
	for i, token := range tokens {
		if i > 0 {
			q = q + " & "
		}
		q = q + token + ":*"
	}
	return q
}
