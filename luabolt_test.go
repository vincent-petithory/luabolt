package luabolt_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/luabolt"
)

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// DB represents a wrapper around a Bolt DB to handle temporary file
// creation and automatic cleanup on close.
type DB struct {
	*bolt.DB
}

// Close closes the database and deletes the underlying file.
func (tdb *DB) Close() error {
	// Close database and remove file.
	p := tdb.DB.Path()
	defer os.Remove(p)
	return tdb.DB.Close()
}

// New returns a new instance of DB.
func NewDB(tb testing.TB) *DB {
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return &DB{DB: db}
}

func setupLuaAndDB(tb testing.TB) (*lua.State, *DB, *bytes.Buffer) {
	db := NewDB(tb)
	l := lua.NewState()
	lua.OpenLibraries(l)
	// register a fprintf func
	var buf bytes.Buffer
	l.Register("fprintf", func(l *lua.State) int {
		return fprintf(l, &buf)
	})
	luabolt.Open(l)
	luabolt.PushDB(l, db.DB, "db")
	return l, db, &buf
}

func fprintf(l *lua.State, w io.Writer) int {
	format := lua.CheckString(l, 1)
	vargs := varArgs(l, 2)
	_, err := fmt.Fprintf(w, format, vargs...)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	return 0
}

func varArgs(l *lua.State, from int) (vargs []interface{}) {
	for i := from; i <= l.Top(); i++ {
		vargs = append(vargs, l.ToValue(i))
	}
	return
}

func TestOpen(t *testing.T) {
	src := `local bolt = require("bolt")
perm = tonumber("666", 8)
db = bolt.open("test1.db", perm)
db.close()
os.remove("test1.db")
`
	l := lua.NewState()
	lua.OpenLibraries(l)

	luabolt.Open(l)
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
}

func TestOpenNilOptions(t *testing.T) {
	src := `local bolt = require("bolt")
perm = tonumber("666", 8)
db = bolt.open("test2.db", perm, nil)
db.close()
os.remove("test2.db")
`
	l := lua.NewState()
	lua.OpenLibraries(l)

	luabolt.Open(l)
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
}

func TestConsts(t *testing.T) {
	tests := []struct {
		luavar string
		name   string
		val    interface{}
	}{
		{luavar: "cnst_mks", name: "max_key_size", val: bolt.MaxKeySize},
		{luavar: "cnst_mvs", name: "max_value_size", val: bolt.MaxValueSize},
		{luavar: "cnst_dmbs", name: "default_max_batch_size", val: bolt.DefaultMaxBatchSize},
		{luavar: "cnst_dmbd", name: "default_max_batch_delay", val: bolt.DefaultMaxBatchDelay},
		{luavar: "cnst_das", name: "default_alloc_size", val: bolt.DefaultAllocSize},
		{luavar: "cnst_dfp", name: "default_fill_percent", val: bolt.DefaultFillPercent},
		{luavar: "cnst_ins", name: "ignore_no_sync", val: bolt.IgnoreNoSync},
	}
	var buf bytes.Buffer
	fmt.Fprintln(&buf, `local bolt = require("bolt")`)
	for _, test := range tests {
		fmt.Fprintf(&buf, "%s = bolt.const(\"%s\")\n", test.luavar, test.name)
	}
	l := lua.NewState()
	lua.OpenLibraries(l)

	luabolt.Open(l)
	if err := lua.DoString(l, buf.String()); err != nil {
		t.Error(err)
		return
	}

	for _, test := range tests {
		l.Global(test.luavar)
		ev := fmt.Sprintf("%v", test.val)
		var v string
		switch t := l.ToValue(-1).(type) {
		case float64:
			if _, ok := test.val.(float64); ok {
				v = fmt.Sprintf("%v", t)
			} else {
				v = fmt.Sprintf("%v", int(t))
			}
		default:
			v = fmt.Sprintf("%v", t)
		}
		if v != ev {
			t.Errorf("%s: expected %s, got %s", test.name, ev, v)
		}
	}
}

func TestUpdateView(t *testing.T) {
	l, db, buf := setupLuaAndDB(t)
	defer db.Close()
	src := `local bolt = require("bolt")
db.update(function(tx)
  b = tx.create_bucket("meow")
  b.put("key", "value")
end)

db.view(function(tx)
  b = tx.bucket("meow")
  v = b.get("key")
  fprintf("%s",v)
end)
`
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
	if es := "value"; buf.String() != es {
		t.Errorf("expected %s, got %s", es, buf.String())
	}
}

func TestCursorForward(t *testing.T) {
	l, db, buf := setupLuaAndDB(t)
	defer db.Close()

	var expectedBuf bytes.Buffer
	l.NewTable()
	for i := range make([]int, 10) {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		fmt.Fprintf(&expectedBuf, "%s:%s\n", k, v)
		l.PushString(v)
		l.SetField(-2, k)
	}
	l.SetGlobal("keys")

	src := `local bolt = require("bolt")

db.update(function(tx)
  b = tx.create_bucket("keys")
  for k, v in pairs(keys) do
    b.put(k, v)
  end
end)

db.view(function(tx)
  b = tx.bucket("keys")
  c = b.cursor()
  k, v = c.first()
  while k do
    fprintf("%s:%s\n", k, v)
    k, v = c.next()
  end
end)
`
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
	if buf.String() != expectedBuf.String() {
		t.Errorf("expected:\n%s\ngot:\n%s", expectedBuf.String(), buf.String())
	}
}

func TestCursorBackward(t *testing.T) {
	l, db, buf := setupLuaAndDB(t)
	defer db.Close()

	var expectedBuf bytes.Buffer
	l.NewTable()
	for i := range make([]int, 10) {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		fmt.Fprintf(&expectedBuf, "%s:%s\n", fmt.Sprintf("k%d", 9-i), fmt.Sprintf("v%d", 9-i))
		l.PushString(v)
		l.SetField(-2, k)
	}
	l.SetGlobal("keys")

	src := `local bolt = require("bolt")

db.update(function(tx)
  b = tx.create_bucket("keys")
  for k, v in pairs(keys) do
    b.put(k, v)
  end
end)

db.view(function(tx)
  b = tx.bucket("keys")
  c = b.cursor()
  k, v = c.last()
  while k do
    fprintf("%s:%s\n", k, v)
    k, v = c.prev()
  end
end)
`
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
	if buf.String() != expectedBuf.String() {
		t.Errorf("expected:\n%s\ngot:\n%s", expectedBuf.String(), buf.String())
	}
}

func TestCursorDeleteSeek(t *testing.T) {
	l, db, buf := setupLuaAndDB(t)
	defer db.Close()

	var expectedBuf bytes.Buffer
	l.NewTable()
	for i := range make([]int, 100) {
		k := fmt.Sprintf("k%.2d", i)
		v := fmt.Sprintf("v%.2d", i)
		l.PushString(v)
		l.SetField(-2, k)
	}
	l.SetGlobal("keys")

	fmt.Fprintf(&expectedBuf, "k42:v42\n")
	fmt.Fprintf(&expectedBuf, "k50:v50\n")
	fmt.Fprintf(&expectedBuf, "k51:v51\n")
	fmt.Fprintf(&expectedBuf, "k90:v90\n")

	src := `local bolt = require("bolt")

db.update(function(tx)
  b = tx.create_bucket("keys")
  for k, v in pairs(keys) do
    b.put(k, v)
  end
end)

db.update(function(tx)
  b = tx.bucket("keys")
  c = b.cursor()
  k, v = c.seek("k42")
  fprintf("%s:%s\n", k, v)
  k, v = c.seek("k5")
  fprintf("%s:%s\n", k, v)
  c.delete()
  k, v = c.next()
  fprintf("%s:%s\n", k, v)
  c2 = c.bucket().cursor()
  k, v = c2.seek("k9")
  fprintf("%s:%s\n", k, v)
end)
`
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
	if buf.String() != expectedBuf.String() {
		t.Errorf("expected:\n%s\ngot:\n%s", expectedBuf.String(), buf.String())
	}
}

func TestTxAndBucket(t *testing.T) {
	l, db, buf := setupLuaAndDB(t)
	defer db.Close()

	var expectedBuf bytes.Buffer
	fmt.Fprintln(&expectedBuf, db.Path())
	fmt.Fprint(&expectedBuf, `writable:true
b2.h:x
b2.h:nil
b21:ok
b22:nil
b22:nil
tx.for_each:b1
tx.for_each:b2
b2.for_each:a,v1
b2.for_each:b21,nil
b2.for_each:c,v2
b1:true
b2:true
b1 deleted:true
`)

	src := `local bolt = require("bolt")

db.update(function(tx)
  dbtx = tx.db()
  fprintf("%s\n", dbtx.path())

  fprintf("writable:%t\n", tx.writable())

  b1 = tx.create_bucket("b1")
  tx.create_bucket_if_not_exists("b1")

  b2 = tx.create_bucket("b2")
  b2.put("a", "v1")
  b2.put("c", "v2")

  b2.put("h", "x")
  v = b2.get("h")
  fprintf("b2.h:%s\n", v)
  b2.delete("h")
  if not b2.get("h") then fprintf("b2.h:nil\n") end

  -- non-existing
  b2.delete("z")

  b2.create_bucket("b21")
  b2.create_bucket_if_not_exists("b21")
  b21 = b2.bucket("b21")
  if b21 then fprintf("b21:ok\n") end
  if not b2.bucket("b22") then fprintf("b22:nil\n") end

  b22 = b2.create_bucket("b22")
  b22.put("x", "y")
  b2.delete_bucket("b22")
  if not b2.bucket("b22") then fprintf("b22:nil\n") end

  tx.for_each(function(name, bucket)
    fprintf("tx.for_each:%s\n", name)
  end)

  b2.for_each(function(k, v)
    vs = "nil"
    if v then vs = v end
    fprintf("b2.for_each:%s,%s\n", k, vs)
  end)

  tc = tx.cursor()
  k, v = tc.first()
  fprintf("%s:%t\n", k, v == nil)
  k, v = tc.next()
  fprintf("%s:%t\n", k, v == nil)
  tx.delete_bucket("b1")
  fprintf("b1 deleted:%t\n", tx.bucket("b1") == nil)
end)
`
	if err := lua.DoString(l, src); err != nil {
		t.Error(err)
		return
	}
	if buf.String() != expectedBuf.String() {
		t.Errorf("expected:\n%s\ngot:\n%s", expectedBuf.String(), buf.String())
	}
}

func TestTxRollback(t *testing.T) {
	// make a writable tx, write something, generate an error, check something is not written
	t.Fatal("not implemented")
}

func TestDBManualTx(t *testing.T) {
	// make a tx with db.begin(), commit / rollback
	t.Fatal("not implemented")
}
