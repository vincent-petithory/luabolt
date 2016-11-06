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
	lmtBucket      = "github.com/boltdb/bolt.Bucket"
	lmtBucketStats = "github.com/boltdb/bolt.BucketStats"
	lmtCursor      = "github.com/boltdb/bolt.Cursor"
	lmtDB          = "github.com/boltdb/bolt.DB"
	lmtInfo        = "github.com/boltdb/bolt.Info"
	lmtOptions     = "github.com/boltdb/bolt.Options"
	lmtPageInfo    = "github.com/boltdb/bolt.PageInfo"
	lmtStats       = "github.com/boltdb/bolt.Stats"
	lmtTx          = "github.com/boltdb/bolt.Tx"
	lmtTxStats     = "github.com/boltdb/bolt.TxStats"
)

var boltOpen = func(l *lua.State) int {
	path := lua.CheckString(l, 1)
	mode := lua.CheckUnsigned(l, 2)
	var options *bolt.Options
	if l.Top() > 2 && !l.IsNil(3) {
		options = lua.CheckUserData(l, 3, lmtOptions).(*bolt.Options)
	}
	db, err := bolt.Open(path, os.FileMode(mode), options)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	l.PushUserData(db)
	lua.SetMetaTableNamed(l, lmtDB)
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
	lua.SetMetaTableNamed(l, lmtDB)
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
