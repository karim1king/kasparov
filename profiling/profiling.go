package profiling

import (
	"net"
	"net/http"

	// Required for profiling
	_ "net/http/pprof"
)

// Start starts the profiling server
func Start(port string) {
	spawn(func() {
		listenAddr := net.JoinHostPort("", port)
		log.Infof("Profile server listening on %s", listenAddr)
		profileRedirect := http.RedirectHandler("/debug/pprof", http.StatusSeeOther)
		http.Handle("/", profileRedirect)
		log.Error(http.ListenAndServe(listenAddr, nil))
	})
}