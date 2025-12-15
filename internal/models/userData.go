package models

type UserData struct {
	UserID    int64  `json:"-"`
	UserName  string `json:"user_name"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}
