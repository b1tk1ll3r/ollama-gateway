package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var embedded embed.FS

func Handler() http.Handler {
	sub, _ := fs.Sub(embedded, "assets")
	return http.FileServer(http.FS(sub))
}
