package luabolt

import (
	"time"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(TypeOptions, optionsFuncs)
}

var optionsFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			options := lua.CheckUserData(l, 1, TypeOptions).(*bolt.Options)
			switch k := lua.CheckString(l, 2); k {
			case "timeout":
				l.PushString(options.Timeout.String())
			case "no_grow_sync":
				l.PushBoolean(options.NoGrowSync)
			case "read_only":
				l.PushBoolean(options.ReadOnly)
			case "mmap_flags":
				l.PushInteger(options.MmapFlags)
			default:
				lua.Errorf(l, "bolt: unknown Options.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			options := lua.CheckUserData(l, 1, TypeOptions).(*bolt.Options)
			switch k := lua.CheckString(l, 2); k {
			case "timeout":
				d, err := time.ParseDuration(lua.CheckString(l, 3))
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("unreachable")
				}
				options.Timeout = d
			case "no_grow_sync":
				lua.CheckType(l, 3, lua.TypeBoolean)
				v := l.ToBoolean(3)
				options.NoGrowSync = v
			case "read_only":
				lua.CheckType(l, 3, lua.TypeBoolean)
				v := l.ToBoolean(3)
				options.ReadOnly = v
			case "mmap_flags":
				options.MmapFlags = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown Options.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
