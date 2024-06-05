package tork

import "time"

type UsernameKey string

const (
	USER_GUEST string      = "guest"
	USERNAME   UsernameKey = "username"
)

type User struct {
	ID           string     `json:"id,omitempty"`
	Name         string     `json:"name,omitempty"`
	Username     string     `json:"username,omitempty"`
	PasswordHash string     `json:"-"`
	Password     string     `json:"password,omitempty"`
	CreatedAt    *time.Time `json:"createdAt,omitempty"`
	Disabled     bool       `json:"disabled,omitempty"`
}

func (u *User) Clone() *User {
	return &User{
		ID:           u.ID,
		Name:         u.Name,
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		CreatedAt:    u.CreatedAt,
		Disabled:     u.Disabled,
	}
}
