package clone

func CloneStringMap(original map[string]string) map[string]string {
	c := make(map[string]string, 0)
	for k, v := range original {
		c[k] = v
	}
	return c
}
