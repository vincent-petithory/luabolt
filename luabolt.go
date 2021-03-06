package luabolt

import (
	"os"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

var registryMTFuncs []func(*lua.State)

func registerMetaTable(name string, funcs []lua.RegistryFunction) {
	registryMTFuncs = append(registryMTFuncs, func(l *lua.State) {
		lua.NewMetaTable(l, name)
		lua.SetFunctions(l, funcs, 0)
	})
}

func Open(l *lua.State) {
	// register metatables
	for _, f := range registryMTFuncs {
		f(l)
	}

	lib := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{"open", boltOpen},
			{"const", boltConst},
		})
		return 1
	}
	lua.Require(l, "bolt", lib, false)
	l.Pop(1)
}

const (
	TypeBucket      = "github.com/boltdb/bolt.Bucket"
	TypeBucketStats = "github.com/boltdb/bolt.BucketStats"
	TypeCursor      = "github.com/boltdb/bolt.Cursor"
	TypeDB          = "github.com/boltdb/bolt.DB"
	TypeInfo        = "github.com/boltdb/bolt.Info"
	TypeOptions     = "github.com/boltdb/bolt.Options"
	TypePageInfo    = "github.com/boltdb/bolt.PageInfo"
	TypeStats       = "github.com/boltdb/bolt.Stats"
	TypeTx          = "github.com/boltdb/bolt.Tx"
	TypeTxStats     = "github.com/boltdb/bolt.TxStats"
)

var boltOpen = func(l *lua.State) int {
	path := lua.CheckString(l, 1)
	mode := lua.CheckUnsigned(l, 2)
	var options *bolt.Options
	if l.Top() > 2 && !l.IsNil(3) {
		options = lua.CheckUserData(l, 3, TypeOptions).(*bolt.Options)
	}
	db, err := bolt.Open(path, os.FileMode(mode), options)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	l.PushUserData(db)
	lua.SetMetaTableNamed(l, TypeDB)
	return 1
}

var boltConst = func(l *lua.State) int {
	switch name := lua.CheckString(l, 1); name {
	case "max_key_size":
		l.PushInteger(bolt.MaxKeySize)
	case "max_value_size":
		l.PushInteger(bolt.MaxValueSize)
	case "default_max_batch_size":
		l.PushInteger(bolt.DefaultMaxBatchSize)
	case "default_max_batch_delay":
		l.PushString(bolt.DefaultMaxBatchDelay.String())
	case "default_alloc_size":
		l.PushInteger(bolt.DefaultAllocSize)
	case "default_fill_percent":
		l.PushNumber(bolt.DefaultFillPercent)
	case "ignore_no_sync":
		l.PushBoolean(bolt.IgnoreNoSync)
	}
	return 1
}

func PushDB(l *lua.State, db *bolt.DB, varName string) {
	if db == nil {
		panic("db is nil")
	}
	l.PushUserData(db)
	lua.SetMetaTableNamed(l, TypeDB)
	l.SetGlobal(varName)
}

func PushTx(l *lua.State, tx *bolt.Tx, varName string) {
	if tx == nil {
		panic("tx is nil")
	}
	l.PushUserData(tx)
	lua.SetMetaTableNamed(l, TypeTx)
	l.SetGlobal(varName)
}

func pushBytes(l *lua.State, v []byte) {
	if v == nil {
		l.PushNil()
	} else {
		s := string(v)
		l.PushString(s)
	}
}

func checkBytes(l *lua.State, index int) []byte {
	s := lua.CheckString(l, index)
	return []byte(s)
}
