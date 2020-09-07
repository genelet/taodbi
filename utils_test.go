package taodbi

import (
	"testing"
)

func TestUtils(t *testing.T) {
	id := int64(1597730628049379)
	str := micro2string(id)

	id1, err := string2micro(str)
	if err != nil { t.Fatal(err) }
	if id != id1 {
		t.Errorf("%d=>%d=>%d", id, id1,id-id1)
	}

	id = int64(1599238790096655)
	str = micro2string(id)

	id1, err = string2micro(str)
	if err != nil { t.Fatal(err) }
	if id != id1 {
		t.Errorf("%d=>%d=>%d", id, id1,(id-id1)/1000000/60/60)
	}
}
