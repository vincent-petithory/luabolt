package luabolt

import (
	"github.com/Shopify/go-lua"
	"github.com/boltdb/bolt"
)

func init() {
	registerMetaTable(TypeBucketStats, bucketStatsFuncs)
}

var bucketStatsFuncs = []lua.RegistryFunction{
	{
		"__index", func(l *lua.State) int {
			bucketStats := lua.CheckUserData(l, 1, TypeBucketStats).(*bolt.BucketStats)
			switch k := lua.CheckString(l, 2); k {
			case "branch_page_n":
				l.PushInteger(bucketStats.BranchPageN)
			case "branch_overflow_n":
				l.PushInteger(bucketStats.BranchOverflowN)
			case "leaf_page_n":
				l.PushInteger(bucketStats.LeafPageN)
			case "leaf_overflow_n":
				l.PushInteger(bucketStats.LeafOverflowN)
			case "key_n":
				l.PushInteger(bucketStats.KeyN)
			case "depth":
				l.PushInteger(bucketStats.Depth)
			case "branch_alloc":
				l.PushInteger(bucketStats.BranchAlloc)
			case "branch_inuse":
				l.PushInteger(bucketStats.BranchInuse)
			case "leaf_alloc":
				l.PushInteger(bucketStats.LeafAlloc)
			case "leaf_inuse":
				l.PushInteger(bucketStats.LeafInuse)
			case "bucket_n":
				l.PushInteger(bucketStats.BucketN)
			case "inline_bucket_n":
				l.PushInteger(bucketStats.InlineBucketN)
			case "inline_bucket_inuse":
				l.PushInteger(bucketStats.InlineBucketInuse)
			case "add":
				l.PushGoFunction(func(l *lua.State) int {
					other := lua.CheckUserData(l, 1, TypeBucketStats).(*bolt.BucketStats)
					bucketStats.Add(*other)
					return 0
				})
			default:
				lua.Errorf(l, "bolt: unknown BucketStats.%s", k)
				panic("unreachable")
			}
			return 1
		},
	},
	{
		"__newindex", func(l *lua.State) int {
			bucketStats := lua.CheckUserData(l, 1, TypeBucketStats).(*bolt.BucketStats)
			switch k := lua.CheckString(l, 2); k {
			case "branch_page_n":
				bucketStats.BranchPageN = lua.CheckInteger(l, 3)
			case "branch_overflow_n":
				bucketStats.BranchOverflowN = lua.CheckInteger(l, 3)
			case "leaf_page_n":
				bucketStats.LeafPageN = lua.CheckInteger(l, 3)
			case "leaf_overflow_n":
				bucketStats.LeafOverflowN = lua.CheckInteger(l, 3)
			case "key_n":
				bucketStats.KeyN = lua.CheckInteger(l, 3)
			case "depth":
				bucketStats.Depth = lua.CheckInteger(l, 3)
			case "branch_alloc":
				bucketStats.BranchAlloc = lua.CheckInteger(l, 3)
			case "branch_inuse":
				bucketStats.BranchInuse = lua.CheckInteger(l, 3)
			case "leaf_alloc":
				bucketStats.LeafAlloc = lua.CheckInteger(l, 3)
			case "leaf_inuse":
				bucketStats.LeafInuse = lua.CheckInteger(l, 3)
			case "bucket_n":
				bucketStats.BucketN = lua.CheckInteger(l, 3)
			case "inline_bucket_n":
				bucketStats.InlineBucketN = lua.CheckInteger(l, 3)
			case "inline_bucket_inuse":
				bucketStats.InlineBucketInuse = lua.CheckInteger(l, 3)
			default:
				lua.Errorf(l, "bolt: unknown BucketStats.%s", k)
				panic("unreachable")
			}
			return 0
		},
	},
}
