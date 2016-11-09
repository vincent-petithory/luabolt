package luabolt

import (
	"time"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(TypeDB, dbFuncs)
}

var dbFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			db := lua.CheckUserData(l, 1, TypeDB).(*bolt.DB)
			switch k := lua.CheckString(l, 2); k {
			case "strict_mode":
				l.PushBoolean(db.StrictMode)
			case "no_sync":
				l.PushBoolean(db.NoSync)
			case "no_grow_sync":
				l.PushBoolean(db.NoGrowSync)
			case "mmap_flags":
				l.PushInteger(db.MmapFlags)
			case "max_batch_size":
				l.PushInteger(db.MaxBatchSize)
			case "max_batch_delay":
				l.PushString(db.MaxBatchDelay.String())
			case "alloc_size":
				l.PushInteger(db.AllocSize)
			case "batch":
				l.PushGoFunction(func(l *lua.State) int {
					lua.CheckType(l, 1, lua.TypeFunction)
					err := db.Batch(func(tx *bolt.Tx) error {
						l.PushValue(1)
						l.PushUserData(tx)
						lua.SetMetaTableNamed(l, TypeTx)
						l.Call(1, 0)
						return nil
					})
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "begin":
				l.PushGoFunction(func(l *lua.State) int {
					lua.CheckType(l, 1, lua.TypeBoolean)
					writable := l.ToBoolean(3)
					tx, err := db.Begin(writable)
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					l.PushUserData(tx)
					lua.SetMetaTableNamed(l, TypeTx)
					return 1
				})
			case "close":
				l.PushGoFunction(func(l *lua.State) int {
					if err := db.Close(); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "go_string":
				l.PushGoFunction(func(l *lua.State) int {
					l.PushString(db.GoString())
					return 1
				})
			case "info":
				l.PushGoFunction(func(l *lua.State) int {
					l.PushUserData(db.Info())
					lua.SetMetaTableNamed(l, TypeInfo)
					return 1
				})
			case "is_read_only":
				l.PushGoFunction(func(l *lua.State) int {
					l.PushBoolean(db.IsReadOnly())
					return 1
				})
			case "path":
				l.PushGoFunction(func(l *lua.State) int {
					l.PushString(db.Path())
					return 1
				})
			case "stats":
				l.PushGoFunction(func(l *lua.State) int {
					stats := db.Stats()
					l.PushUserData(&stats)
					lua.SetMetaTableNamed(l, TypeStats)
					return 1
				})
			case "string":
				l.PushGoFunction(func(l *lua.State) int {
					l.PushString(db.String())
					return 1
				})
			case "sync":
				l.PushGoFunction(func(l *lua.State) int {
					if err := db.Sync(); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "update":
				l.PushGoFunction(func(l *lua.State) int {
					lua.CheckType(l, 1, lua.TypeFunction)
					err := db.Update(func(tx *bolt.Tx) error {
						l.PushUserData(tx)
						lua.SetMetaTableNamed(l, TypeTx)
						l.Call(1, 0)
						return nil
					})
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "view":
				l.PushGoFunction(func(l *lua.State) int {
					lua.CheckType(l, 1, lua.TypeFunction)
					err := db.View(func(tx *bolt.Tx) error {
						l.PushUserData(tx)
						lua.SetMetaTableNamed(l, TypeTx)
						l.Call(1, 0)
						return nil
					})
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			default:
				lua.Errorf(l, "bolt: unknown DB.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			db := lua.CheckUserData(l, 1, TypeDB).(*bolt.DB)
			switch k := lua.CheckString(l, 2); k {
			case "strict_mode":
				lua.CheckType(l, 3, lua.TypeBoolean)
				v := l.ToBoolean(3)
				db.StrictMode = v
			case "no_sync":
				lua.CheckType(l, 3, lua.TypeBoolean)
				v := l.ToBoolean(3)
				db.NoSync = v
			case "no_grow_sync":
				lua.CheckType(l, 3, lua.TypeBoolean)
				v := l.ToBoolean(3)
				db.NoGrowSync = v
			case "mmap_flags":
				db.MmapFlags = lua.CheckInteger(l, 3)
			case "max_batch_size":
				db.MaxBatchSize = lua.CheckInteger(l, 3)
			case "max_batch_delay":
				s := lua.CheckString(l, 3)
				d, err := time.ParseDuration(s)
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("unreachable")
				}
				db.MaxBatchDelay = d
			case "alloc_size":
				db.AllocSize = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown DB.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
