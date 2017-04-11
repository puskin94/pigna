package webServer

import (
	"log"
	"net/http"

	"github.com/zenazn/goji"
	"github.com/zenazn/goji/web"
)

func hello(c web.C, w http.ResponseWriter, r *http.Request) {
	log.Println(w, "Hello, %s!", c.URLParams["name"])
}

func StartServer() {
	goji.Get("/hello/:name", hello)
	goji.Serve()
}
