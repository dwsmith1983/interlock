package archetype

import (
	"fmt"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ValidateArchetype checks that an archetype definition is well-formed.
func ValidateArchetype(arch *types.Archetype) error {
	if arch.Name == "" {
		return fmt.Errorf("archetype name is required")
	}
	if len(arch.RequiredTraits) == 0 {
		return fmt.Errorf("at least one required trait is needed")
	}

	seen := make(map[string]bool)
	for _, trait := range arch.RequiredTraits {
		if trait.Type == "" {
			return fmt.Errorf("trait type is required")
		}
		if seen[trait.Type] {
			return fmt.Errorf("duplicate trait type %q", trait.Type)
		}
		seen[trait.Type] = true
	}
	for _, trait := range arch.OptionalTraits {
		if trait.Type == "" {
			return fmt.Errorf("optional trait type is required")
		}
		if seen[trait.Type] {
			return fmt.Errorf("duplicate trait type %q", trait.Type)
		}
		seen[trait.Type] = true
	}

	if arch.ReadinessRule.Type == "" {
		return fmt.Errorf("readinessRule.type is required")
	}
	if arch.ReadinessRule.Type != types.AllRequiredPass {
		return fmt.Errorf("unsupported readiness rule type %q", arch.ReadinessRule.Type)
	}

	return nil
}
