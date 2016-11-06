package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(lmtInfo, infoFuncs)
}

var infoFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			info := lua.CheckUserData(l, 1, lmtInfo).(*bolt.Info)
			switch k := lua.CheckString(l, 2); k {
			case "data":
				l.PushUnsigned(uint(info.Data))
			case "page_size":
				l.PushInteger(info.PageSize)
			default:
				lua.Errorf(l, "bolt: unknown Info.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			info := lua.CheckUserData(l, 1, lmtInfo).(*bolt.Info)
			switch k := lua.CheckString(l, 2); k {
			case "data":
				info.Data = uintptr(lua.CheckUnsigned(l, 3))
			case "page_size":
				info.PageSize = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown Info.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
