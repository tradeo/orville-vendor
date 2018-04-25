package metrics

import (
	"net/http"

	metrics "github.com/rcrowley/go-metrics"
)

// Handler provides http handler for metrics reporting.
func Handler(mreg metrics.Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		metrics.WriteJSONOnce(mreg, w)
	})
}
