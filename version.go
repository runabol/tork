package tork

import "fmt"

const (
	Version = "0.1.16"
)

var (
	GitCommit string = "develop"
)

func FormattedVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
