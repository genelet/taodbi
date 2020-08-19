package taodbi

import (
	"testing"
)

func TestUtils(t *testing.T) {
	id := int64(1597730628049379)
	str := micro2string(id)

	id1, err := string2micro(str)
	if err != nil { t.Fatal(err) }
	t.Errorf("%d=>%d", id, id1)
}
