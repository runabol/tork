package tork

import "fmt"

const (
	Version = "0.1.30"
)

var (
	GitCommit string = "develop"
)

func FormattedVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
