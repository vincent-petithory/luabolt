package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(lmtBucket, bucketFuncs)
}

var bucketFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			bucket := lua.CheckUserData(l, 1, lmtBucket).(*bolt.Bucket)
			switch k := lua.CheckString(l, 2); k {
			case "fill_percent":
				l.PushNumber(bucket.FillPercent)
			case "bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					b := bucket.Bucket(name)
					if b == nil {
						l.PushNil()
					} else {
						l.PushUserData(b)
						lua.SetMetaTableNamed(l, lmtBucket)
					}
					return 1
				})
			case "create_bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					b, err := bucket.CreateBucket(name)
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
					b, err := bucket.CreateBucketIfNotExists(name)
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
					c := bucket.Cursor()
					l.PushUserData(c)
					lua.SetMetaTableNamed(l, lmtCursor)
					return 1
				})
			case "delete":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					if err := bucket.Delete(name); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "delete_bucket":
				l.PushGoFunction(func(l *lua.State) int {
					name := checkBytes(l, 1)
					if err := bucket.DeleteBucket(name); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "for_each":
				l.PushGoFunction(func(l *lua.State) int {
					// TODO should we expose the inner error to lua?
					lua.CheckType(l, 1, lua.TypeFunction)
					err := bucket.ForEach(func(k, v []byte) error {
						l.PushValue(1)
						pushBytes(l, k)
						pushBytes(l, v)
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
			case "get":
				l.PushGoFunction(func(l *lua.State) int {
					k := checkBytes(l, 1)
					v := bucket.Get(k)
					pushBytes(l, v)
					return 1
				})
			case "next_sequence":
				l.PushGoFunction(func(l *lua.State) int {
					i, err := bucket.NextSequence()
					if err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					l.PushUnsigned(uint(i))
					return 1
				})
			case "put":
				l.PushGoFunction(func(l *lua.State) int {
					k := checkBytes(l, 1)
					v := checkBytes(l, 2)
					if err := bucket.Put(k, v); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "root":
				l.PushGoFunction(func(l *lua.State) int {
					i := bucket.Root()
					l.PushUnsigned(uint(i))
					return 1
				})
			case "sequence":
				l.PushGoFunction(func(l *lua.State) int {
					i := bucket.Sequence()
					l.PushUnsigned(uint(i))
					return 1
				})
			case "set_sequence":
				l.PushGoFunction(func(l *lua.State) int {
					i := lua.CheckUnsigned(l, 1)
					if err := bucket.SetSequence(uint64(i)); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "stats":
				l.PushGoFunction(func(l *lua.State) int {
					stats := bucket.Stats()
					l.PushUserData(&stats)
					lua.SetMetaTableNamed(l, lmtBucketStats)
					return 1
				})
			case "tx":
				l.PushGoFunction(func(l *lua.State) int {
					tx := bucket.Tx()
					l.PushUserData(&tx)
					lua.SetMetaTableNamed(l, lmtTx)
					return 1
				})
			case "writable":
				l.PushGoFunction(func(l *lua.State) int {
					b := bucket.Writable()
					l.PushBoolean(b)
					return 1
				})
			default:
				lua.Errorf(l, "bolt: unknown Bucket.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			bucket := lua.CheckUserData(l, 1, lmtBucket).(*bolt.Bucket)
			switch k := lua.CheckString(l, 2); k {
			case "fill_percent":
				bucket.FillPercent = lua.CheckNumber(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown Bucket.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
