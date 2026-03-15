package analyticsdeps

import (
	"net/http"

	"github.com/benbjohnson/clock"
)

// Deps contains shared runtime dependencies used to construct analytics modules.
type Deps struct {
	HTTPClient *http.Client
	Clock      clock.Clock
}