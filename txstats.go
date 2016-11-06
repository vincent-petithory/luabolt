package luabolt

import (
	"time"

	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(lmtTxStats, txStatsFuncs)
}

var txStatsFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			txStats := lua.CheckUserData(l, 1, lmtTxStats).(*bolt.TxStats)
			switch k := lua.CheckString(l, 2); k {
			case "page_count":
				l.PushInteger(txStats.PageCount)
			case "page_alloc":
				l.PushInteger(txStats.PageAlloc)
			case "cursor_count":
				l.PushInteger(txStats.CursorCount)
			case "node_count":
				l.PushInteger(txStats.NodeCount)
			case "node_deref":
				l.PushInteger(txStats.NodeDeref)
			case "rebalance":
				l.PushInteger(txStats.Rebalance)
			case "rebalance_time":
				l.PushString(txStats.RebalanceTime.String())
			case "split":
				l.PushInteger(txStats.Split)
			case "spill":
				l.PushInteger(txStats.Spill)
			case "spill_time":
				l.PushString(txStats.SpillTime.String())
			case "write":
				l.PushInteger(txStats.Write)
			case "write_time":
				l.PushString(txStats.WriteTime.String())
			case "sub":
				l.PushGoFunction(func(l *lua.State) int {
					other := lua.CheckUserData(l, 1, lmtTxStats).(*bolt.TxStats)
					sub := txStats.Sub(other)
					l.PushUserData(&sub)
					lua.SetMetaTableNamed(l, lmtTxStats)
					return 1
				})
			default:
				lua.Errorf(l, "bolt: unknown TxStats.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			txStats := lua.CheckUserData(l, 1, lmtTxStats).(*bolt.TxStats)
			switch k := lua.CheckString(l, 2); k {
			case "page_count":
				txStats.PageCount = lua.CheckInteger(l, 3)
			case "page_alloc":
				txStats.PageAlloc = lua.CheckInteger(l, 3)
			case "cursor_count":
				txStats.CursorCount = lua.CheckInteger(l, 3)
			case "node_count":
				txStats.NodeCount = lua.CheckInteger(l, 3)
			case "node_deref":
				txStats.NodeDeref = lua.CheckInteger(l, 3)
			case "rebalance":
				txStats.Rebalance = lua.CheckInteger(l, 3)
			case "rebalance_time":
				d, err := time.ParseDuration(lua.CheckString(l, 3))
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("unreachable")
				}
				txStats.RebalanceTime = d
			case "split":
				txStats.Split = lua.CheckInteger(l, 3)
			case "spill":
				txStats.Spill = lua.CheckInteger(l, 3)
			case "spill_time":
				d, err := time.ParseDuration(lua.CheckString(l, 3))
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("unreachable")
				}
				txStats.SpillTime = d
			case "write":
				txStats.Write = lua.CheckInteger(l, 3)
			case "write_time":
				d, err := time.ParseDuration(lua.CheckString(l, 3))
				if err != nil {
					lua.Errorf(l, err.Error())
					panic("unreachable")
				}
				txStats.WriteTime = d
			default:
				lua.Errorf(l, "bolt: unknown TxStats.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
