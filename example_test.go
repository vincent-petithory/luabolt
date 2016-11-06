package luabolt_test

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/luabolt"
)

func ExampleExistingDB() {
	f, _ := ioutil.TempFile("", "bolt-")
	_ = f.Close()
	_ = os.Remove(f.Name())
	dbPath := f.Name()

	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	}()

	// Add some keys in the db
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("arrows"))
		if err != nil {
			return err
		}
		for _, kv := range []struct {
			k string
			v string
		}{
			{k: "0.standard", v: "3"},
			{k: "1.bomb", v: "4"},
			{k: "2.laser", v: "2"},
			{k: "3.bramble", v: "5"},
			{k: "4.drill", v: "1"},
			{k: "5.bolt", v: "2"},
			{k: "6.super_bomb", v: "2"},
			{k: "7.feather", v: "7"},
			{k: "8.trigger", v: "2"},
			{k: "9.prism", v: "4"},
		} {
			if err := b.Put([]byte(kv.k), []byte(kv.v)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// create a lua thread and open standard libraries
	l := lua.NewState()

	// at least base and package are necessary.
	lua.BaseOpen(l)
	lua.PackageOpen(l)
	// or open all libraries
	//lua.OpenLibraries(l)

	// open luabolt library
	luabolt.Open(l)

	// inject the db
	luabolt.PushDB(l, db, "db")

	src := `local bolt = require("bolt")
-- iterate on all arrows, add +1 to their count, and print the new total
local total = 0
db.update(function(tx)
  b = tx.bucket("arrows")
  c = b.cursor()
  k, v = c.first()
  while k do
    i = tonumber(v, 10)
    i=i+1
    total = total+i
    b.put(k, tostring(i))
    k, v = c.next()
  end
end)
print(total)
`
	if err := lua.DoString(l, src); err != nil {
		log.Fatal(err)
	}
	// Output:
	// 42
}
