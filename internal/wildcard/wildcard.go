package wildcard

// credit: https://github.com/vodkaslime/wildcard

const C = '*'

func isWildPattern(pattern string) bool {
	for i := range pattern {
		c := pattern[i]
		if c == C {
			return true
		}
	}

	return false
}

func Match(pattern string, s string) bool {
	// Edge cases.
	if pattern == string(C) {
		return true
	}

	if pattern == "" {
		return s == ""
	}

	// If pattern does not contain wildcard chars, just compare the strings
	// to avoid extra memory allocation.
	if !isWildPattern(pattern) {
		return pattern == s
	}

	// Initialize DP.
	lp := len(pattern)
	ls := len(s)
	dp := make([][]bool, lp+1)
	for i := 0; i < lp+1; i++ {
		dp[i] = make([]bool, ls+1)
	}

	dp[0][0] = true

	for i := 0; i < lp; i++ {
		if pattern[i] == C {
			dp[i+1][0] = dp[i][0]
		} else {
			dp[i+1][0] = false
		}
	}

	for j := 0; j < ls; j++ {
		dp[0][j+1] = false
	}

	// Start DP.
	for i := 0; i < lp; i++ {
		for j := 0; j < ls; j++ {
			pc := pattern[i]
			sc := s[j]
			switch pattern[i] {
			case C:
				dp[i+1][j+1] = dp[i][j] || dp[i][j+1] || dp[i+1][j]
			default:
				if pc == sc {
					dp[i+1][j+1] = dp[i][j]
				} else {
					dp[i+1][j+1] = false
				}
			}
		}
	}

	return dp[lp][ls]
}
