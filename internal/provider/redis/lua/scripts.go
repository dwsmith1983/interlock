// Package lua embeds Lua scripts for Redis/Valkey operations.
package lua

import _ "embed"

//go:embed compare_and_swap.lua
var CompareAndSwap string
