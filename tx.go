package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(lmtTx, txFuncs)
}

var txFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			tx := lua.CheckUserData(l, 1, lmtTx).(*bolt.Tx)
			switch k := lua.CheckString(l, 2); k {
			case "write_flag":
				l.PushInteger(tx.WriteFlag)
			case "bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					b := tx.Bucket(name)
					if b == nil {
						l.PushNil()
					} else {
						l.PushUserData(b)
						lua.SetMetaTableNamed(l, lmtBucket)
					}
					return 1
				})
			case "check":
				l.PushGoFunction(func(l *lua.State) int {
					err := <-tx.Check()
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "commit":
				l.PushGoFunction(func(l *lua.State) int {
					if err := tx.Commit(); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "copy", "write_to":
				// TODO impl
				panic("not implemented")
			case "copy_file":
				// TODO impl
				panic("not implemented")
			case "create_bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					b, err := tx.CreateBucket(name)
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					l.PushUserData(b)
					lua.SetMetaTableNamed(l, lmtBucket)
					return 1
				})
			case "create_bucket_if_not_exists":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					b, err := tx.CreateBucketIfNotExists(name)
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					l.PushUserData(b)
					lua.SetMetaTableNamed(l, lmtBucket)
					return 1
				})
			case "cursor":
				l.PushGoFunction(func(l *lua.State) int {
					c := tx.Cursor()
					l.PushUserData(c)
					lua.SetMetaTableNamed(l, lmtCursor)
					return 1
				})
			case "db":
				l.PushGoFunction(func(l *lua.State) int {
					db := tx.DB()
					l.PushUserData(db)
					lua.SetMetaTableNamed(l, lmtDB)
					return 1
				})
			case "delete_bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					if err := tx.DeleteBucket(name); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "for_each":
				l.PushGoFunction(func(l *lua.State) int {
					// TODO should we expose the inner error to lua?
					lua.CheckType(l, 1, lua.TypeFunction)
					err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
						l.PushValue(1)
						pushBytes(l, name)
						l.PushUserData(b)
						lua.SetMetaTableNamed(l, lmtBucket)
						l.Call(2, 0)
						return nil
					})
					l.Pop(1)
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "id":
				l.PushGoFunction(func(l *lua.State) int {
					id := tx.ID()
					l.PushInteger(id)
					return 1
				})
			case "on_commit":
				l.PushGoFunction(func(l *lua.State) int {
					tx.OnCommit(func() {
						lua.CheckType(l, 1, lua.TypeFunction)
						l.Call(0, 0)
					})
					return 0
				})
			case "page_info":
				l.PushGoFunction(func(l *lua.State) int {
					id := lua.CheckInteger(l, 1)
					pi, err := tx.Page(id)
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					l.PushUserData(pi)
					lua.SetMetaTableNamed(l, lmtPageInfo)
					return 1
				})
			case "rollback":
				l.PushGoFunction(func(l *lua.State) int {
					if err := tx.Rollback(); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "size":
				l.PushGoFunction(func(l *lua.State) int {
					i := tx.Size()
					l.PushInteger(int(i))
					return 1
				})
			case "stats":
				l.PushGoFunction(func(l *lua.State) int {
					stats := tx.Stats()
					l.PushUserData(&stats)
					lua.SetMetaTableNamed(l, lmtTxStats)
					return 1
				})
			case "writable":
				l.PushGoFunction(func(l *lua.State) int {
					b := tx.Writable()
					l.PushBoolean(b)
					return 1
				})
			default:
				lua.Errorf(l, "bolt: unknown Tx.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			tx := lua.CheckUserData(l, 1, lmtTx).(*bolt.Tx)
			switch k := lua.CheckString(l, 2); k {
			case "write_flag":
				tx.WriteFlag = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown Tx.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
