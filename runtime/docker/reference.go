package docker

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// NameTotalLengthMax is the maximum total number of characters in a repository name.
	nameTotalLengthMax = 255
)

var (
	// alphaNumericRegexp defines the alpha numeric atom, typically a
	// component of names. This only allows lower case characters and digits.
	alphaNumericRegexp = match(`[a-z0-9]+`)

	// separatorRegexp defines the separators allowed to be embedded in name
	// components. This allow one period, one or two underscore and multiple
	// dashes.
	separatorRegexp = match(`(?:[._]|__|[-]*)`)

	// nameComponentRegexp restricts registry path component names to start
	// with at least one letter or number, with following parts able to be
	// separated by one period, one or two underscore and multiple dashes.
	nameComponentRegexp = expression(
		alphaNumericRegexp,
		optional(repeated(separatorRegexp, alphaNumericRegexp)))

	// domainComponentRegexp restricts the registry domain component of a
	// repository name to start with a component as defined by DomainRegexp
	// and followed by an optional port.
	domainComponentRegexp = match(`(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])`)

	// DomainRegexp defines the structure of potential domain components
	// that may be part of image names. This is purposely a subset of what is
	// allowed by DNS to ensure backwards compatibility with Docker image
	// names.
	domainRegexp = expression(
		domainComponentRegexp,
		optional(repeated(literal(`.`), domainComponentRegexp)),
		optional(literal(`:`), match(`[0-9]+`)))

	// TagRegexp matches valid tag names. From docker/docker:graph/tags.go.
	tagRegexp = match(`[\w][\w.-]{0,127}`)

	// DigestRegexp matches valid digests.
	digestRegexp = match(`[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*[:][[:xdigit:]]{32,}`)

	// NameRegexp is the format for the name component of references. The
	// regexp has capturing groups for the domain and name part omitting
	// the separating forward slash from either.
	nameRegexp = expression(
		optional(domainRegexp, literal(`/`)),
		nameComponentRegexp,
		optional(repeated(literal(`/`), nameComponentRegexp)))

	// anchoredNameRegexp is used to parse a name value, capturing the
	// domain and trailing components.
	anchoredNameRegexp = anchored(
		optional(capture(domainRegexp), literal(`/`)),
		capture(nameComponentRegexp,
			optional(repeated(literal(`/`), nameComponentRegexp))))

	// ReferenceRegexp is the full supported format of a reference. The regexp
	// is anchored and has capturing groups for name, tag, and digest
	// components.
	referenceRegexp = anchored(capture(nameRegexp),
		optional(literal(":"), capture(tagRegexp)),
		optional(literal("@"), capture(digestRegexp)))
)

var (
	// ErrReferenceInvalidFormat represents an error while trying to parse a string as a reference.
	errReferenceInvalidFormat = errors.New("invalid reference format")

	// ErrNameContainsUppercase is returned for invalid repository names that contain uppercase characters.
	errNameContainsUppercase = errors.New("repository name must be lowercase")

	// ErrNameEmpty is returned for empty, invalid repository names.
	errNameEmpty = errors.New("repository name must have at least one component")

	// ErrNameTooLong is returned when a repository name is longer than NameTotalLengthMax.
	errNameTooLong = fmt.Errorf("repository name must not be more than %v characters", nameTotalLengthMax)
)

// match compiles the string to a regular expression.
var match = regexp.MustCompile

type reference struct {
	domain string
	path   string
	tag    string
}

// Parse parses s and returns a syntactically valid Reference.
func parseRef(s string) (reference, error) {
	matches := referenceRegexp.FindStringSubmatch(s)
	if matches == nil {
		if s == "" {
			return reference{}, errNameEmpty
		}
		if referenceRegexp.FindStringSubmatch(strings.ToLower(s)) != nil {
			return reference{}, errNameContainsUppercase
		}
		return reference{}, errReferenceInvalidFormat
	}

	if len(matches[1]) > nameTotalLengthMax {
		return reference{}, errNameTooLong
	}

	ref := reference{
		tag: matches[2],
	}

	nameMatch := anchoredNameRegexp.FindStringSubmatch(matches[1])
	if len(nameMatch) == 3 {
		ref.domain = nameMatch[1]
		ref.path = nameMatch[2]
	} else {
		ref.domain = ""
		ref.path = matches[1]
	}

	return ref, nil
}

// literal compiles s into a literal regular expression, escaping any regexp
// reserved characters.
func literal(s string) *regexp.Regexp {
	re := match(regexp.QuoteMeta(s))

	if _, complete := re.LiteralPrefix(); !complete {
		panic("must be a literal")
	}

	return re
}

// expression defines a full expression, where each regular expression must
// follow the previous.
func expression(res ...*regexp.Regexp) *regexp.Regexp {
	var s string
	for _, re := range res {
		s += re.String()
	}

	return match(s)
}

// optional wraps the expression in a non-capturing group and makes the
// production optional.
func optional(res ...*regexp.Regexp) *regexp.Regexp {
	return match(group(expression(res...)).String() + `?`)
}

// repeated wraps the regexp in a non-capturing group to get one or more
// matches.
func repeated(res ...*regexp.Regexp) *regexp.Regexp {
	return match(group(expression(res...)).String() + `+`)
}

// group wraps the regexp in a non-capturing group.
func group(res ...*regexp.Regexp) *regexp.Regexp {
	return match(`(?:` + expression(res...).String() + `)`)
}

// capture wraps the expression in a capturing group.
func capture(res ...*regexp.Regexp) *regexp.Regexp {
	return match(`(` + expression(res...).String() + `)`)
}

// anchored anchors the regular expression by adding start and end delimiters.
func anchored(res ...*regexp.Regexp) *regexp.Regexp {
	return match(`^` + expression(res...).String() + `$`)
}
