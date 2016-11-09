package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(TypePageInfo, pageInfoFuncs)
}

var pageInfoFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			pageInfo := lua.CheckUserData(l, 1, TypePageInfo).(*bolt.PageInfo)
			switch k := lua.CheckString(l, 2); k {
			case "id":
				l.PushInteger(pageInfo.ID)
			case "type":
				l.PushString(pageInfo.Type)
			case "count":
				l.PushInteger(pageInfo.Count)
			case "overflow_count":
				l.PushInteger(pageInfo.OverflowCount)
			default:
				lua.Errorf(l, "bolt: unknown PageInfo.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			pageInfo := lua.CheckUserData(l, 1, TypePageInfo).(*bolt.PageInfo)
			switch k := lua.CheckString(l, 2); k {
			case "id":
				pageInfo.ID = lua.CheckInteger(l, 3)
			case "type":
				pageInfo.Type = lua.CheckString(l, 3)
			case "count":
				pageInfo.Count = lua.CheckInteger(l, 3)
			case "overflow_count":
				pageInfo.OverflowCount = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown PageInfo.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
