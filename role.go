package tork

import "time"

const (
	ROLE_PUBLIC string = "public"
)

type Role struct {
	ID        string     `json:"id,omitempty"`
	Slug      string     `json:"slug,omitempty"`
	Name      string     `json:"name,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
}

func (r *Role) Clone() *Role {
	return &Role{
		ID:        r.ID,
		Slug:      r.Slug,
		Name:      r.Name,
		CreatedAt: r.CreatedAt,
	}
}

type UserRole struct {
	ID        string     `json:"id,omitempty"`
	UserID    string     `json:"userId,omitempty"`
	RoleID    string     `json:"roleId,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
}
