package clocksweep

import (
	"errors"
	"reflect"
	"testing"
)

func TestClockSweep(t *testing.T) {
	cs := NewClockSweep[int, string](2)

	{
		err := cs.Set(1, "one")
		assertNil(t, err)
		assertDeepEqual(t, cs.frames[0], &Frame[int, string]{
			cnt:    1,
			refCnt: 0,
			key:    1,
			value:  "one",
		})
	}

	{
		got, release, err := cs.Acquire(1)
		assertNil(t, err)
		assertEqual(t, "one", *got)

		assertDeepEqual(t, cs.frames[0], &Frame[int, string]{
			cnt:    2,
			refCnt: 1,
			key:    1,
			value:  "one",
		})

		release()

		assertDeepEqual(t, cs.frames[0], &Frame[int, string]{
			cnt:    2,
			refCnt: 0,
			key:    1,
			value:  "one",
		})

		got, release, err = cs.Acquire(2)
		assertEqualError(t, ErrKeyNotFound, err)
		assertNil(t, release)
		assertNil(t, got)

		assertDeepEqual(t, cs.frames[0], &Frame[int, string]{
			cnt:    1,
			refCnt: 0,
			key:    1,
			value:  "one",
		})
	}

	{
		err := cs.Set(2, "two")
		assertNil(t, err)

		assertDeepEqual(t, cs.frames[1], &Frame[int, string]{
			cnt:    1,
			refCnt: 0,
			key:    2,
			value:  "two",
		})
	}

	got1, release1, err := cs.Acquire(1)
	assertEqual(t, "one", *got1)
	assertNil(t, err)

	assertDeepEqual(t, cs.frames[0], &Frame[int, string]{
		cnt:    2,
		refCnt: 1,
		key:    1,
		value:  "one",
	})

	{
		err := cs.Set(3, "three")
		assertNil(t, err)

		assertDeepEqual(t, cs.frames[1], &Frame[int, string]{
			cnt:    1,
			refCnt: 0,
			key:    3,
			value:  "three",
		})
	}

	release1()
}

func assertEqual[U comparable](t *testing.T, got, want U) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertDeepEqual[U comparable](t *testing.T, got, want U) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertEqualError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertNil(t *testing.T, got any) {
	t.Helper()

	if got != nil && !reflect.ValueOf(got).IsNil() {
		t.Errorf("got %v, want nil", got)
	}

}
