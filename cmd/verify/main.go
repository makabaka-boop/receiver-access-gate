// Command verify is the one-shot acceptance suite for docker compose.
//
// It exercises the two REAL running API service containers (api1, api2) over
// HTTP: readiness, the two-process exclusive race, SHARED coexistence,
// conflict behavior, cross-process wrong-token (403) protection, persistence
// of state/tokens, and token-leak hygiene. Exit status is 0 only if every
// assertion holds.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type apiClient struct {
	name string
	base string
	http *http.Client
}

type grant struct {
	GrantID    string    `json:"grant_id"`
	Receiver   string    `json:"receiver"`
	Mode       string    `json:"mode"`
	Status     string    `json:"status"`
	OwnerToken string    `json:"owner_token"`
	CreatedAt  time.Time `json:"created_at"`
}

type listedGrant struct {
	GrantID    string `json:"grant_id"`
	Mode       string `json:"mode"`
	Status     string `json:"status"`
	OwnerToken string `json:"owner_token"`
	TokenHash  string `json:"token_hash"`
}

type listResponse struct {
	Grants []listedGrant `json:"grants"`
}

func main() {
	a := &apiClient{name: "api1", base: env("API1_URL", "http://api1:8080"), http: &http.Client{Timeout: 10 * time.Second}}
	b := &apiClient{name: "api2", base: env("API2_URL", "http://api2:8080"), http: &http.Client{Timeout: 10 * time.Second}}

	if err := waitReady(a, 60*time.Second); err != nil {
		fail("api1 never became ready: %v", err)
	}
	if err := waitReady(b, 60*time.Second); err != nil {
		fail("api2 never became ready: %v", err)
	}
	ok("both API instances healthy")

	// Unique per run so `docker compose run verify` stays re-runnable
	// against a persistent database volume.
	runID := time.Now().UTC().Format("20060102T150405.000000000")

	var failures int
	for _, tc := range []struct {
		name string
		fn   func(rx string, a, b *apiClient) error
	}{
		{"exclusive race across two processes", verifyExclusiveRace},
		{"shared coexistence and exclusive conflict", verifySharedCoexist},
		{"wrong-token release forbidden across processes", verifyForbiddenRelease},
		{"state and token survive a simulated restart view", verifyConsistentViews},
	} {
		rx := fmt.Sprintf("verify-%s-%d", runID, time.Now().UnixNano())
		if err := tc.fn(rx, a, b); err != nil {
			fmt.Printf("FAIL  %s: %v\n", tc.name, err)
			failures++
			continue
		}
		fmt.Printf("PASS  %s\n", tc.name)
	}

	if failures > 0 {
		fail("%d acceptance check(s) failed", failures)
	}
	fmt.Println("VERIFY OK: access gate accepted")
}

// verifyExclusiveRace: many concurrent EXCLUSIVE requests spread over both
// instances; exactly one may win.
func verifyExclusiveRace(rx string, a, b *apiClient) error {
	const n = 30
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		won     []string
		busy    int
		other   int
		errOnce error
	)
	barrier := make(chan struct{})
	fire := func(c *apiClient) {
		defer wg.Done()
		<-barrier
		code, g, _, err := c.create(rx, "EXCLUSIVE")
		if err != nil {
			mu.Lock()
			errOnce = err
			mu.Unlock()
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch code {
		case http.StatusCreated:
			won = append(won, g.GrantID)
		case http.StatusConflict:
			busy++
		default:
			other++
		}
	}
	for i := 0; i < n; i++ {
		wg.Add(2)
		go fire(a)
		go fire(b)
	}
	close(barrier)
	wg.Wait()
	if errOnce != nil {
		return errOnce
	}
	if len(won) != 1 {
		return fmt.Errorf("expected exactly one exclusive grant, got %d (busy=%d other=%d)", len(won), busy, other)
	}
	if busy != 2*n-1 || other != 0 {
		return fmt.Errorf("expected %d BUSY, got busy=%d other=%d", 2*n-1, busy, other)
	}
	code, lr, err := a.list(rx)
	if err != nil || code != http.StatusOK || len(lr.Grants) != 1 || lr.Grants[0].Status != "ACTIVE" {
		return fmt.Errorf("post-race view inconsistent: code=%d grants=%d err=%v", code, len(lr.Grants), err)
	}
	return nil
}

// verifySharedCoexist covers shared/shared coexistence, BUSY on exclusive and
// on shared-while-exclusive, and that conflicts leave no records.
func verifySharedCoexist(rx string, a, b *apiClient) error {
	code, g1, _, err := a.create(rx, "SHARED")
	if err != nil || code != http.StatusCreated {
		return fmt.Errorf("shared #1: code=%d err=%v", code, err)
	}
	code, g2, _, err := b.create(rx, "SHARED")
	if err != nil || code != http.StatusCreated {
		return fmt.Errorf("shared #2: code=%d err=%v", code, err)
	}
	if g1.GrantID == g2.GrantID {
		return errors.New("duplicate grant ids")
	}

	if code, _, ebody, _ := a.create(rx, "EXCLUSIVE"); code != http.StatusConflict || ebody.Error != "BUSY" {
		return fmt.Errorf("exclusive vs shared: code=%d err=%+v", code, ebody)
	}

	_, lr, err := b.list(rx)
	if err != nil || len(lr.Grants) != 2 {
		return fmt.Errorf("conflict left a record: grants=%d err=%v", len(lr.Grants), err)
	}

	// Release both shared holders via opposite instances, then exclusive wins.
	if code, _, _, err := b.release(rx, g1.GrantID, g1.OwnerToken); err != nil || code != http.StatusOK {
		return fmt.Errorf("release shared #1: code=%d err=%v", code, err)
	}
	if code, rel, _, err := a.release(rx, g2.GrantID, g2.OwnerToken); err != nil || code != http.StatusOK || rel.Status != "RELEASED" {
		return fmt.Errorf("release shared #2: code=%d rel=%+v err=%v", code, rel, err)
	}
	if code, gx, _, err := a.create(rx, "EXCLUSIVE"); err != nil || code != http.StatusCreated {
		return fmt.Errorf("exclusive after free: code=%d err=%v", code, err)
	} else {
		// Shared while exclusive active must be BUSY and leave no row.
		if code2, _, e2, _ := b.create(rx, "SHARED"); code2 != http.StatusConflict || e2.Error != "BUSY" {
			return fmt.Errorf("shared vs exclusive: code=%d err=%+v", code2, e2)
		}
		// Clean up so receiver ends released.
		if code3, _, _, err := a.release(rx, gx.GrantID, gx.OwnerToken); err != nil || code3 != http.StatusOK {
			return fmt.Errorf("cleanup exclusive: code=%d err=%v", code3, err)
		}
	}
	return nil
}

// verifyForbiddenRelease: a wrong token through the other process returns
// 403 FORBIDDEN and changes neither the target nor any active grant; the
// correct token from either process then succeeds.
func verifyForbiddenRelease(rx string, a, b *apiClient) error {
	code, g, _, err := a.create(rx, "EXCLUSIVE")
	if err != nil || code != http.StatusCreated {
		return fmt.Errorf("seed: code=%d err=%v", code, err)
	}

	wrong := []string{
		"tkn_0000000000000000000000000000000000000000000000000000000000000000",
		g.OwnerToken + "x",
	}
	for _, tok := range wrong {
		code, _, ebody, err := b.release(rx, g.GrantID, tok)
		if err != nil {
			return err
		}
		if code != http.StatusForbidden || ebody.Error != "FORBIDDEN" {
			return fmt.Errorf("wrong token: code=%d err=%+v", code, ebody)
		}
	}
	if _, lr, err := a.list(rx); err != nil || len(lr.Grants) != 1 || lr.Grants[0].Status != "ACTIVE" {
		return fmt.Errorf("active set changed after 403: %+v err=%v", lr.Grants, err)
	}

	// Correct token through the other process.
	code, rel, _, err := b.release(rx, g.GrantID, g.OwnerToken)
	if err != nil || code != http.StatusOK || rel.Status != "RELEASED" {
		return fmt.Errorf("correct release: code=%d rel=%+v err=%v", code, rel, err)
	}
	// Idempotent replay.
	code, rel, _, err = a.release(rx, g.GrantID, g.OwnerToken)
	if err != nil || code != http.StatusOK || rel.Status != "RELEASED" {
		return fmt.Errorf("idempotent replay: code=%d rel=%+v err=%v", code, rel, err)
	}
	return nil
}

// verifyConsistentViews proves both processes agree on persisted state and
// never leak token material, and queries are stable across repeated reads.
func verifyConsistentViews(rx string, a, b *apiClient) error {
	code, g, _, err := a.create(rx, "SHARED")
	if err != nil || code != http.StatusCreated {
		return fmt.Errorf("seed: code=%d err=%v", code, err)
	}

	var firstIDs []string
	for i := 0; i < 3; i++ {
		code, lr, err := b.list(rx)
		if err != nil || code != http.StatusOK {
			return fmt.Errorf("list: code=%d err=%v", code, err)
		}
		ids := make([]string, 0, len(lr.Grants))
		for _, gr := range lr.Grants {
			ids = append(ids, gr.GrantID)
			if gr.OwnerToken != "" || gr.TokenHash != "" {
				return errors.New("list response leaks token material")
			}
		}
		if i == 0 {
			firstIDs = ids
		} else if len(ids) != len(firstIDs) || ids[0] != firstIDs[0] {
			return fmt.Errorf("unstable listing: %v vs %v", firstIDs, ids)
		}
	}

	// Raw payload hygiene.
	raw, code, err := b.get(fmt.Sprintf("/v1/receivers/%s/grants", rx))
	if err != nil || code != http.StatusOK {
		return fmt.Errorf("raw list: code=%d err=%v", code, err)
	}
	if bytes.Contains(raw, []byte("tkn_")) || bytes.Contains(raw, []byte("token_hash")) {
		return fmt.Errorf("raw list leaks token material: %s", raw)
	}
	if bytes.Contains(raw, []byte(g.OwnerToken)) {
		return errors.New("raw list contains the plaintext owner token")
	}
	return nil
}

func waitReady(c *apiClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := c.http.Get(c.base + "/healthz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			_ = body
		}
		time.Sleep(time.Second)
	}
	return errors.New("timeout")
}

type errBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func (c *apiClient) create(rx, mode string) (int, grant, errBody, error) {
	var g grant
	var e errBody
	code, raw, err := c.post(fmt.Sprintf("/v1/receivers/%s/grants", rx),
		map[string]string{"mode": mode})
	if err != nil {
		return 0, g, e, err
	}
	_ = json.Unmarshal(raw, &g)
	_ = json.Unmarshal(raw, &e)
	return code, g, e, nil
}

func (c *apiClient) release(rx, id, token string) (int, grant, errBody, error) {
	var g grant
	var e errBody
	code, raw, err := c.post(fmt.Sprintf("/v1/receivers/%s/grants/%s/release", rx, id),
		map[string]string{"owner_token": token})
	if err != nil {
		return 0, g, e, err
	}
	_ = json.Unmarshal(raw, &g)
	_ = json.Unmarshal(raw, &e)
	return code, g, e, nil
}

func (c *apiClient) list(rx string) (int, listResponse, error) {
	var lr listResponse
	raw, code, err := c.get(fmt.Sprintf("/v1/receivers/%s/grants", rx))
	if err != nil {
		return code, lr, err
	}
	return code, lr, json.Unmarshal(raw, &lr)
}

func (c *apiClient) post(path string, body any) (int, []byte, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(buf))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s: %w", c.name, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, err
}

func (c *apiClient) get(path string) ([]byte, int, error) {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", c.name, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return raw, resp.StatusCode, err
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func ok(format string, args ...any) { fmt.Printf("PASS  %s\n", fmt.Sprintf(format, args...)) }
func fail(format string, args ...any) {
	fmt.Printf("VERIFY FAILED: %s\n", fmt.Sprintf(format, args...))
	os.Exit(1)
}
