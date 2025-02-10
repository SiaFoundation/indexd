package testutils

import (
	"testing"

	"go.uber.org/goleak"
)

type (
	// TT is a wrapper around testing.T that provides additional helper methods.
	TT interface {
		TestingCommon

		OK(err error)
		OKAll(vs ...interface{})
	}

	// TestingCommon is an interface that describes the common methods of
	// testing.T and testing.B ensuring this testutil can be used in both
	// contexts.
	TestingCommon interface {
		Log(args ...any)
		Logf(format string, args ...any)
		Error(args ...any)
		Errorf(format string, args ...any)
		Fatal(args ...any)
		Fatalf(format string, args ...any)
		Skip(args ...any)
		Skipf(format string, args ...any)
		SkipNow()
		Skipped() bool
		Helper()
		Cleanup(f func())
		TempDir() string
		Setenv(key, value string)
	}

	impl struct {
		TestingCommon
	}
)

// NewTT returns a new TT.
func NewTT(tc TestingCommon) TT {
	return &impl{TestingCommon: tc}
}

func (t impl) OK(err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (t impl) OKAll(vs ...interface{}) {
	t.Helper()
	for _, v := range vs {
		if err, ok := v.(error); ok && err != nil {
			t.Fatal(err)
		}
	}
}

// VerifyTestMain contains any setup and teardown code that should be run
// before/after running all tests that belong to a package.
func VerifyTestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
