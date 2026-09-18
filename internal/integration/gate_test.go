package integration

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
)

// TestSharedCoexist covers the shared watchers rule: multiple ACTIVE SHARED
// grants coexist, an EXCLUSIVE request against them gets 409 BUSY without
// leaving a row, and releasing the shared holders eventually lets an
// exclusive through.
func TestSharedCoexist(t *testing.T) {
	a, b := startTwo(t)
	rx := "rx-shared-01"
	ca, cb := a.cl(), b.cl()

	code, g1, e := ca.create(t, rx, "SHARED")
	if code != http.StatusCreated {
		t.Fatalf("first shared: code=%d err=%+v", code, e)
	}
	if g1.OwnerToken == "" || g1.Status != "ACTIVE" || g1.Mode != "SHARED" {
		t.Fatalf("bad first grant: %+v", g1)
	}

	code, g2, e := cb.create(t, rx, "SHARED")
	if code != http.StatusCreated {
		t.Fatalf("second shared via second process: code=%d err=%+v", code, e)
	}
	if g1.GrantID == g2.GrantID {
		t.Fatal("grant ids must be unique")
	}

	// Both processes must see the same stable view.
	for _, c := range []*client{ca, cb} {
		code, lr := c.list(t, rx)
		if code != http.StatusOK || len(lr.Grants) != 2 {
			t.Fatalf("list: code=%d n=%d", code, len(lr.Grants))
		}
		for _, g := range lr.Grants {
			if g.Status != "ACTIVE" || g.Mode != "SHARED" {
				t.Fatalf("unexpected grant %+v", g)
			}
			if g.OwnerToken != "" || g.TokenHash != "" {
				t.Fatal("query must never expose token material")
			}
		}
		if lr.Grants[0].GrantID != g1.GrantID || lr.Grants[1].GrantID != g2.GrantID {
			t.Fatalf("list order not stable by created_at,id: %s then %s",
				lr.Grants[0].GrantID, lr.Grants[1].GrantID)
		}
		if raw := rawList(t, c, rx); bytes.Contains(raw, []byte("tkn_")) ||
			bytes.Contains(raw, []byte("token_hash")) {
			t.Fatalf("list response leaks token material: %s", raw)
		}
	}

	// Exclusive while shared are active: conflict, and no row may remain.
	code, _, e = ca.create(t, rx, "EXCLUSIVE")
	if code != http.StatusConflict || e.Error != "BUSY" {
		t.Fatalf("want 409 BUSY, got %d %+v", code, e)
	}
	if _, lr := ca.list(t, rx); len(lr.Grants) != 2 {
		t.Fatal("conflict request must not leave a record")
	}

	// Release one shared holder: still one active, exclusive still blocked.
	if code, rel, e := ca.release(t, rx, g1.GrantID, g1.OwnerToken); code != http.StatusOK ||
		rel.Status != "RELEASED" {
		t.Fatalf("release g1: code=%d rel=%+v err=%+v", code, rel, e)
	}
	if code, _, e := cb.create(t, rx, "EXCLUSIVE"); code != http.StatusConflict ||
		e.Error != "BUSY" {
		t.Fatalf("exclusive with 1 active shared: code=%d err=%+v", code, e)
	}

	// Release the last shared holder: exclusive now succeeds and shared is
	// blocked in turn.
	if code, _, _ := cb.release(t, rx, g2.GrantID, g2.OwnerToken); code != http.StatusOK {
		t.Fatalf("release g2: %d", code)
	}
	code, g3, e := ca.create(t, rx, "EXCLUSIVE")
	if code != http.StatusCreated || g3.Mode != "EXCLUSIVE" {
		t.Fatalf("exclusive after release: code=%d err=%+v", code, e)
	}
	code, _, e = cb.create(t, rx, "SHARED")
	if code != http.StatusConflict || e.Error != "BUSY" {
		t.Fatalf("shared against active exclusive: code=%d err=%+v", code, e)
	}

	// The failed shared attempt left nothing; view includes the two
	// released and one active grant.
	_, lr := ca.list(t, rx)
	if len(lr.Grants) != 3 {
		t.Fatalf("want 3 total rows, got %d", len(lr.Grants))
	}
	active, released := 0, 0
	for _, g := range lr.Grants {
		switch g.Status {
		case "ACTIVE":
			active++
		case "RELEASED":
			released++
		}
	}
	if active != 1 || released != 2 {
		t.Fatalf("want 1 ACTIVE / 2 RELEASED, got %d / %d", active, released)
	}
}

// TestConcurrentExclusiveRace is the core two-process gate test: many
// EXCLUSIVE applications fired at the same receiver through two API
// instances concurrently. Exactly one must be created.
func TestConcurrentExclusiveRace(t *testing.T) {
	a, b := startTwo(t)
	rx := "rx-race-exclusive"

	const perInstance = 24
	type result struct {
		code int
		id   string
	}
	resCh := make(chan result, perInstance*2)
	barrier := make(chan struct{})
	var wg sync.WaitGroup

	fire := func(c *client) {
		defer wg.Done()
		<-barrier
		code, g, _ := c.create(t, rx, "EXCLUSIVE")
		resCh <- result{code: code, id: g.GrantID}
	}
	for range perInstance {
		wg.Add(2)
		go fire(a.cl())
		go fire(b.cl())
	}
	close(barrier)
	wg.Wait()
	close(resCh)

	created, busy := 0, 0
	ids := map[string]bool{}
	for r := range resCh {
		switch r.code {
		case http.StatusCreated:
			created++
			ids[r.id] = true
		case http.StatusConflict:
			busy++
		default:
			t.Fatalf("unexpected status %d", r.code)
		}
	}
	if created != 1 || len(ids) != 1 {
		t.Fatalf("double exclusive: created=%d distinct=%d, busy=%d", created, len(ids), busy)
	}
	if busy != perInstance*2-1 {
		t.Fatalf("want %d BUSY, got %d", perInstance*2-1, busy)
	}

	// Persisted view must show exactly one ACTIVE row.
	_, lr := b.cl().list(t, rx)
	active := 0
	for _, g := range lr.Grants {
		if g.Status == "ACTIVE" {
			active++
		}
	}
	if active != 1 || len(lr.Grants) != 1 {
		t.Fatalf("persisted grants after race: %+v", lr.Grants)
	}
}

// TestConcurrentMixedRace: with one SHARED already active, a burst mixing
// EXCLUSIVE and SHARED requests through both processes must produce only
// SHARED successes and BUSY for every EXCLUSIVE attempt.
func TestConcurrentMixedRace(t *testing.T) {
	a, b := startTwo(t)
	rx := "rx-race-mixed"

	code, first, _ := a.cl().create(t, rx, "SHARED")
	if code != http.StatusCreated {
		t.Fatalf("seed shared: %d", code)
	}

	const each = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	sharedOK, exclusiveBusy := 0, 0
	barrier := make(chan struct{})

	fire := func(c *client, mode string) {
		defer wg.Done()
		<-barrier
		code, _, e := c.create(t, rx, mode)
		mu.Lock()
		defer mu.Unlock()
		switch mode {
		case "SHARED":
			if code != http.StatusCreated {
				t.Errorf("shared burst: code=%d err=%s", code, e.Error)
			}
			sharedOK++
		case "EXCLUSIVE":
			if code != http.StatusConflict || e.Error != "BUSY" {
				t.Errorf("exclusive burst: code=%d err=%s", code, e.Error)
			}
			exclusiveBusy++
		}
	}
	for range each {
		wg.Add(2)
		go fire(a.cl(), "SHARED")
		go fire(b.cl(), "EXCLUSIVE")
	}
	close(barrier)
	wg.Wait()

	if sharedOK != each || exclusiveBusy != each {
		t.Fatalf("sharedOK=%d exclusiveBusy=%d", sharedOK, exclusiveBusy)
	}
	_, lr := a.cl().list(t, rx)
	for _, g := range lr.Grants {
		if g.Mode != "SHARED" || g.Status != "ACTIVE" {
			t.Fatalf("only ACTIVE SHARED rows expected, got %+v", g)
		}
	}
	if len(lr.Grants) != each+1 {
		t.Fatalf("want %d active shared, got %d", each+1, len(lr.Grants))
	}
	_ = first
}

// TestReleaseAuthorization covers correct-token release, wrong-token 403
// FORBIDDEN with the target record and active set untouched, idempotent
// re-release, and cross-receiver lookups.
func TestReleaseAuthorization(t *testing.T) {
	a, b := startTwo(t)
	rx := "rx-release-01"

	code, g, _ := a.cl().create(t, rx, "EXCLUSIVE")
	if code != http.StatusCreated {
		t.Fatalf("seed exclusive: %d", code)
	}

	// Wrong token through the *other* process: 403, nothing changes.
	for _, bad := range []string{
		"tkn_deadbeef",
		"tkn_0000000000000000000000000000000000000000000000000000000000000000",
		"not-a-token",
	} {
		code, _, e := b.cl().release(t, rx, g.GrantID, bad)
		if code != http.StatusForbidden || e.Error != "FORBIDDEN" {
			t.Fatalf("wrong token: code=%d err=%+v", code, e)
		}
	}
	_, lr := a.cl().list(t, rx)
	if len(lr.Grants) != 1 || lr.Grants[0].Status != "ACTIVE" {
		t.Fatalf("active set must be unchanged after 403: %+v", lr.Grants)
	}

	// Another receiver remains unaffected too.
	code, other, _ := b.cl().create(t, "rx-release-other", "SHARED")
	if code != http.StatusCreated {
		t.Fatalf("other receiver should stay free: %d", code)
	}

	// Cross-receiver path for an existing grant: 404, unchanged.
	code, _, e := a.cl().release(t, "rx-release-other", g.GrantID, g.OwnerToken)
	if code != http.StatusNotFound || e.Error != "NOT_FOUND" {
		t.Fatalf("cross-receiver release: code=%d err=%+v", code, e)
	}

	// Unknown grant id: 404.
	code, _, e = b.cl().release(t, rx, "g_00000000000000000000000000000000", g.OwnerToken)
	if code != http.StatusNotFound {
		t.Fatalf("missing grant: code=%d err=%+v", code, e)
	}

	// Correct token from the other process releases it.
	code, rel, e := b.cl().release(t, rx, g.GrantID, g.OwnerToken)
	if code != http.StatusOK || rel.Status != "RELEASED" {
		t.Fatalf("correct release: code=%d rel=%+v err=%+v", code, rel, e)
	}

	// Wrong token against the now-released record: still 403, still RELEASED.
	code, _, e = a.cl().release(t, rx, g.GrantID, "tkn_wrong")
	if code != http.StatusForbidden {
		t.Fatalf("wrong token post-release: code=%d err=%+v", code, e)
	}

	// Correct token again: idempotent success, no new rows.
	code, rel, _ = a.cl().release(t, rx, g.GrantID, g.OwnerToken)
	if code != http.StatusOK || rel.Status != "RELEASED" {
		t.Fatalf("idempotent re-release: code=%d rel=%+v", code, rel)
	}
	if _, lr := b.cl().list(t, rx); len(lr.Grants) != 1 {
		t.Fatalf("re-release must not add rows: %+v", lr.Grants)
	}

	// The other receiver's grant is untouched.
	_, lrOther := a.cl().list(t, "rx-release-other")
	if len(lrOther.Grants) != 1 || lrOther.Grants[0].Status != "ACTIVE" ||
		lrOther.Grants[0].GrantID != other.GrantID {
		t.Fatalf("other receiver changed unexpectedly: %+v", lrOther.Grants)
	}
}

// TestPersistenceAcrossRestart proves state lives in PostgreSQL: brand-new
// API processes (fresh pools/servers, as after a full restart) see the grant
// and the originally issued token still governs release.
func TestPersistenceAcrossRestart(t *testing.T) {
	a, _ := startTwo(t)
	rx := "rx-restart-01"

	code, g, e := a.cl().create(t, rx, "EXCLUSIVE")
	if code != http.StatusCreated {
		t.Fatalf("seed: code=%d err=%+v", code, e)
	}

	// Start two "restarted" processes without truncating anything.
	c := startInstance(t, "api-C-restarted")
	d := startInstance(t, "api-D-restarted")

	code, lr := c.cl().list(t, rx)
	if code != http.StatusOK || len(lr.Grants) != 1 ||
		lr.Grants[0].GrantID != g.GrantID || lr.Grants[0].Status != "ACTIVE" {
		t.Fatalf("restarted process view: code=%d grants=%+v", code, lr.Grants)
	}

	// Original token still valid after restart.
	code, rel, e := d.cl().release(t, rx, g.GrantID, g.OwnerToken)
	if code != http.StatusOK || rel.Status != "RELEASED" {
		t.Fatalf("release after restart: code=%d rel=%+v err=%+v", code, rel, e)
	}

	// Wrong token after restart is still rejected.
	code, _, e = c.cl().release(t, rx, g.GrantID, g.OwnerToken+"x")
	if code != http.StatusForbidden || e.Error != "FORBIDDEN" {
		t.Fatalf("wrong token after restart: code=%d err=%+v", code, e)
	}
}

func rawList(t *testing.T, c *client, rx string) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/v1/receivers/%s/grants", c.base, rx), nil)
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(raw, []byte(`"grants"`)) {
		t.Fatalf("unexpected list payload: %s", raw)
	}
	return raw
}
