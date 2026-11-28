//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd

package auditlog

func Open(string, string) (*Store, error)        { return nil, ErrUnavailable }
func (*Store) Begin(Event) (*Transaction, error) { return nil, ErrUnavailable }
func (*Store) Record(Event) error                { return ErrUnavailable }
func (*Store) finish(Event) error                { return ErrUnavailable }
func (*Store) ListRecent(int) ([]Event, error)   { return nil, ErrUnavailable }
func (*Store) Close() error                      { return ErrUnavailable }
