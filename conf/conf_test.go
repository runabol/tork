package conf_test

import (
	"os"
	"testing"

	"github.com/runabol/tork/conf"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfigNotExist(t *testing.T) {
	err := conf.LoadConfig()
	assert.NoError(t, err)
}

func TestLoadConfigNotExistUserDefined(t *testing.T) {
	err := conf.LoadConfig("no.such.file.toml")
	assert.Error(t, err)
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
	err = conf.LoadConfig("myconfig.toml")
	assert.Error(t, err)
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
