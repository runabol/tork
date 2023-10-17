package conf_test

import (
	"os"
	"testing"
	"time"

	"github.com/runabol/tork/conf"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfigNotExist(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)
}

func TestLoadConfigNotExistUserDefined(t *testing.T) {
	os.Setenv("TORK_CONFIG", "no.such.thing")
	defer func() {
		os.Unsetenv("TORK_CONFIG")
	}()
	err := conf.LoadConfig()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not find config file in: no.such.thing")
}

func TestLoadConfigBadContents(t *testing.T) {
	err := os.WriteFile("config.toml", []byte("xyz"), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.Error(t, err)
}

func TestLoadConfigValid(t *testing.T) {
	konf := "[main]\nkey1 = value1"
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.Error(t, err)
}

func TestString(t *testing.T) {
	konf := `
	[main]
	key1 = "value1"
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.Equal(t, "value1", conf.String("main.key1"))
}

func TestStrings(t *testing.T) {
	konf := `
	[main]
	keys = ["value1"]
	`
	err := os.WriteFile("config_strings.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config_strings.toml"))
	}()
	os.Setenv("TORK_CONFIG", "config_strings.toml")
	defer func() {
		os.Unsetenv("TORK_CONFIG")
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.Equal(t, []string{"value1"}, conf.Strings("main.keys"))
}

func TestStringsEnv(t *testing.T) {
	os.Setenv("TORK_MAIN_STRINGS_KEYS", "a,b,c")
	defer func() {
		os.Unsetenv("TORK_MAIN_STRINGS_KEYS")
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, conf.Strings("main.strings.keys"))
}

func TestStringDefault(t *testing.T) {
	konf := `
	[main]
	key1 = "value1"
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.Equal(t, "v2", conf.StringDefault("main.key2", "v2"))
}

func TestIntMap(t *testing.T) {
	konf := `
	[main]
	map.key1 = 1
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.Equal(t, map[string]int(map[string]int{"key1": 1}), conf.IntMap("main.map"))
}

func TestLoadConfigCustomPath(t *testing.T) {
	konf := "[main]\nkey1 = value1"
	err := os.WriteFile("myconfig.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("myconfig.toml"))
	}()
	os.Setenv("TORK_CONFIG", "myconfig.toml")
	defer func() {
		os.Unsetenv("TORK_CONFIG")
	}()
	err = conf.LoadConfig()
	assert.Error(t, err)
}

func TestLoadConfigWithOverridingEnv(t *testing.T) {
	konf := `
	[main]
	key1 = "value1"
	key3 = "value3"
	`
	err := os.WriteFile("config_with_override.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config_with_override.toml"))
	}()
	os.Setenv("TORK_CONFIG", "config_with_override.toml")
	defer func() {
		os.Unsetenv("TORK_CONFIG")
	}()
	assert.NoError(t, os.Setenv("TORK_MAIN_KEY1", "value2"))
	defer func() {
		assert.NoError(t, os.Unsetenv("TORK_MAIN_KEY1"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)

	assert.Equal(t, "value2", conf.String("main.key1"))
	assert.Equal(t, "value3", conf.String("main.key3"))
}

func TestLoadConfigEnv(t *testing.T) {
	assert.NoError(t, os.Setenv("TORK_HELLO", "world"))
	defer func() {
		assert.NoError(t, os.Unsetenv("TORK_HELLO"))
	}()
	err := conf.LoadConfig()
	assert.NoError(t, err)

	assert.Equal(t, "world", conf.String("hello"))
}

func TestBoolTrue(t *testing.T) {
	konf := `
	[main]
	enabled = true
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.True(t, conf.Bool("main.enabled"))
}

func TestBoolFalse(t *testing.T) {
	konf := `
	[main]
	enabled = false
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()
	assert.NoError(t, err)
	assert.False(t, conf.Bool("main.enabled"))
}

func TestBoolDefault(t *testing.T) {
	konf := `
	[main]
	enabled = false
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()

	assert.NoError(t, err)
	assert.False(t, conf.BoolDefault("main.enabled", true))
	assert.False(t, conf.BoolDefault("main.enabled", false))
	assert.True(t, conf.BoolDefault("main.other", true))
}

func TestDurationDefault(t *testing.T) {
	konf := `
	[main]
	some.duration = "5m"
	`
	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()
	err = conf.LoadConfig()

	assert.NoError(t, err)
	assert.Equal(t, time.Minute*5, conf.DurationDefault("main.some.duration", time.Minute))
	assert.Equal(t, time.Minute, conf.DurationDefault("main.other.duration", time.Minute))
}

func TestBoolMap(t *testing.T) {
	os.Setenv("TORK_BOOLMAP_KEY1", "false")
	os.Setenv("TORK_BOOLMAP_KEY2", "true")
	defer func() {
		os.Unsetenv("TORK_BOOLMAP_KEY1")
		os.Unsetenv("TORK_BOOLMAP_KEY2")
	}()

	err := conf.LoadConfig()
	assert.NoError(t, err)

	m := conf.BoolMap("boolmap")

	assert.False(t, m["key1"])
	assert.True(t, m["key2"])
}

func TestUnmarshal(t *testing.T) {
	konf := `
	[main]
	str1 = "value1"
	bool1 = true
	sarr1 = ["a","b"]
	`

	err := os.WriteFile("config.toml", []byte(konf), os.ModePerm)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.Remove("config.toml"))
	}()

	err = conf.LoadConfig()
	assert.NoError(t, err)

	type MyConfig struct {
		Str1  string   `koanf:"str1"`
		Str2  string   `koanf:"str2"`
		Bool1 bool     `koanf:"bool1"`
		SArr1 []string `koanf:"sarr1"`
		SArr2 []string `koanf:"sarr2"`
	}

	c := &MyConfig{
		Str2:  "default",
		SArr2: []string{"default1", "default2"},
	}
	err = conf.Unmarshal("main", &c)
	assert.NoError(t, err)

	assert.Equal(t, "value1", c.Str1)
	assert.Equal(t, "default", c.Str2)
	assert.True(t, c.Bool1)
	assert.Equal(t, []string{"a", "b"}, c.SArr1)
	assert.Equal(t, []string{"default1", "default2"}, c.SArr2)
}
