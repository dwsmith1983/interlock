package evaluator_test

import (
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
)

// Compile-time interface satisfaction checks.
var (
	_ engine.TraitRunner = (*evaluator.Runner)(nil)
	_ engine.TraitRunner = (*evaluator.HTTPRunner)(nil)
)
