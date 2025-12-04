package models

type UserData struct {
	UserID    int64  `json:"UserID"`
	UserName  string `json:"UserName"`
	FirstName string `json:"FirstName"`
	LastName  string `json:"LastName"`
}
