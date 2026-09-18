package store

import "context"

// TruncateForTest removes all grant data. Test infrastructure only.
func (s *Store) TruncateForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "TRUNCATE grants")
	return err
}
