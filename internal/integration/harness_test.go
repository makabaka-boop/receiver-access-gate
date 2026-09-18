package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"accessgate/internal/httpapi"
	"accessgate/internal/store"
)

const testDSNEnv = "GATE_TEST_DATABASE_URL"

// apiInstance is one real HTTP server backed by its own store/pool, as if it
// were a separate service process.
type apiInstance struct {
	t    *testing.T
	srv  *httptest.Server
	db   *store.Store
	name string
}

type client struct {
	http *http.Client
	base string
	inst string
}

func startInstance(t *testing.T, name string) *apiInstance {
	t.Helper()
	dsn := os.Getenv(testDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set; skipping real-database integration test", testDSNEnv)
	}

	db, err := store.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("%s: connect: %v", name, err)
	}
	t.Cleanup(db.Close)

	srv := httptest.NewServer(httpapi.NewHandler(db))
	t.Cleanup(srv.Close)

	return &apiInstance{t: t, srv: srv, db: db, name: name}
}

func (a *apiInstance) cl() *client {
	return &client{http: a.srv.Client(), base: a.srv.URL, inst: a.name}
}

type grantResp struct {
	GrantID    string    `json:"grant_id"`
	Receiver   string    `json:"receiver"`
	Mode       string    `json:"mode"`
	Status     string    `json:"status"`
	OwnerToken string    `json:"owner_token"`
	CreatedAt  time.Time `json:"created_at"`
}

type apiErr struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func (c *client) create(t *testing.T, receiver, mode string) (int, grantResp, apiErr) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"mode": mode})
	code, raw := c.do(t, http.MethodPost,
		fmt.Sprintf("/v1/receivers/%s/grants", receiver), body)

	var g grantResp
	var e apiErr
	_ = json.Unmarshal(raw, &g)
	_ = json.Unmarshal(raw, &e)
	return code, g, e
}

func (c *client) release(t *testing.T, receiver, grantID, token string) (int, grantResp, apiErr) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"owner_token": token})
	code, raw := c.do(t, http.MethodPost,
		fmt.Sprintf("/v1/receivers/%s/grants/%s/release", receiver, grantID), body)

	var g grantResp
	var e apiErr
	_ = json.Unmarshal(raw, &g)
	_ = json.Unmarshal(raw, &e)
	return code, g, e
}

type listResp struct {
	Receiver string `json:"receiver"`
	Grants   []struct {
		GrantID string `json:"grant_id"`
		Mode    string `json:"mode"`
		Status  string `json:"status"`
		// These two must never be populated: token material is not exposed.
		OwnerToken string    `json:"owner_token"`
		TokenHash  string    `json:"token_hash"`
		CreatedAt  time.Time `json:"created_at"`
	} `json:"grants"`
}

func (c *client) list(t *testing.T, receiver string) (int, listResp) {
	t.Helper()
	code, raw := c.do(t, http.MethodGet,
		fmt.Sprintf("/v1/receivers/%s/grants", receiver), nil)
	var lr listResp
	if err := json.Unmarshal(raw, &lr); err != nil {
		t.Fatalf("%s: list decode %d: %v\nbody: %s", c.inst, code, err, raw)
	}
	return code, lr
}

func (c *client) do(t *testing.T, method, path string, body []byte) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("%s: %s %s: %v", c.inst, method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

// resetDB truncates all grant data before a test. Advisory locks are
// transaction-scoped and need no cleanup.
func resetDB(t *testing.T, insts ...*apiInstance) {
	t.Helper()
	if len(insts) == 0 {
		t.Fatal("need at least one instance to reset DB")
	}
	if err := insts[0].db.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := insts[0].db.TruncateForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startTwo(t *testing.T) (*apiInstance, *apiInstance) {
	t.Helper()
	a := startInstance(t, "api-A")
	b := startInstance(t, "api-B")
	resetDB(t, a, b)
	return a, b
}
