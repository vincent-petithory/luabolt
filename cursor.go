package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(lmtCursor, cursorFuncs)
}

var cursorFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			cursor := lua.CheckUserData(l, 1, lmtCursor).(*bolt.Cursor)
			switch k := lua.CheckString(l, 2); k {
			case "bucket":
				l.PushGoFunction(func(l *lua.State) int {
					b := cursor.Bucket()
					l.PushUserData(b)
					lua.SetMetaTableNamed(l, lmtBucket)
					return 1
				})
			case "delete":
				l.PushGoFunction(func(l *lua.State) int {
					if err := cursor.Delete(); err != nil {
						lua.Errorf(l, err.Error())
						panic("unreachable")
					}
					return 0
				})
			case "first":
				l.PushGoFunction(func(l *lua.State) int {
					k, v := cursor.First()
					pushBytes(l, k)
					pushBytes(l, v)
					return 2
				})
			case "last":
				l.PushGoFunction(func(l *lua.State) int {
					k, v := cursor.Last()
					pushBytes(l, k)
					pushBytes(l, v)
					return 2
				})
			case "next":
				l.PushGoFunction(func(l *lua.State) int {
					k, v := cursor.Next()
					pushBytes(l, k)
					pushBytes(l, v)
					return 2
				})
			case "prev":
				l.PushGoFunction(func(l *lua.State) int {
					k, v := cursor.Prev()
					pushBytes(l, k)
					pushBytes(l, v)
					return 2
				})
			case "seek":
				l.PushGoFunction(func(l *lua.State) int {
					seek := checkBytes(l, 1)
					k, v := cursor.Seek(seek)
					pushBytes(l, k)
					pushBytes(l, v)
					return 2
				})
			default:
				lua.Errorf(l, "bolt: unknown Cursor.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			return 0
		},
	},
}
