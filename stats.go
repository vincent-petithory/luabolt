package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(TypeStats, statsFuncs)
}

var statsFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			stats := lua.CheckUserData(l, 1, TypeStats).(*bolt.Stats)
			switch k := lua.CheckString(l, 2); k {
			case "free_page_n":
				l.PushInteger(stats.FreePageN)
			case "pending_page_n":
				l.PushInteger(stats.PendingPageN)
			case "free_alloc":
				l.PushInteger(stats.FreeAlloc)
			case "freelist_inuse":
				l.PushInteger(stats.FreelistInuse)
			case "tx_n":
				l.PushInteger(stats.TxN)
			case "open_tx_n":
				l.PushInteger(stats.OpenTxN)
			case "tx_stats":
				l.PushUserData(&stats.TxStats)
				lua.SetMetaTableNamed(l, TypeTxStats)
			case "sub":
				l.PushGoFunction(func(l *lua.State) int {
					other := lua.CheckUserData(l, 1, TypeStats).(*bolt.Stats)
					sub := stats.Sub(other)
					l.PushUserData(&sub)
					lua.SetMetaTableNamed(l, TypeStats)
					return 1
				})
			default:
				lua.Errorf(l, "bolt: unknown Stats.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			stats := lua.CheckUserData(l, 1, TypeStats).(*bolt.Stats)
			switch k := lua.CheckString(l, 2); k {
			case "free_page_n":
				stats.FreePageN = lua.CheckInteger(l, 3)
			case "pending_page_n":
				stats.PendingPageN = lua.CheckInteger(l, 3)
			case "free_alloc":
				stats.FreeAlloc = lua.CheckInteger(l, 3)
			case "freelist_inuse":
				stats.FreelistInuse = lua.CheckInteger(l, 3)
			case "tx_n":
				stats.TxN = lua.CheckInteger(l, 3)
			case "open_tx_n":
				stats.OpenTxN = lua.CheckInteger(l, 3)
			case "tx_stats":
				txStats := lua.CheckUserData(l, 1, TypeDB).(*bolt.TxStats)
				stats.TxStats = *txStats
			default:
				lua.Errorf(l, "bolt: unknown Stats.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
