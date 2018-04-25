package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/metrics", nil)
	assert.NoError(t, err)

	mreg := metrics.NewRegistry()
	gauge := metrics.GetOrRegisterGauge("/test", mreg)
	gauge.Update(42)

	rr := httptest.NewRecorder()
	handler := http.Handler(Handler(mreg))

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	assert.Equal(t, "application/json; charset=utf-8", rr.Header().Get("Content-Type"))

	expected := `{"/test":{"value":42}}` + "\n"
	assert.Equal(t, expected, rr.Body.String())
}
