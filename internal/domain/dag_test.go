package domain

import (
	"errors"
	"testing"
)

// makeDeps — вспомогательная функция: строит getDeps из простого map.
func makeDeps(m map[TaskID][]TaskID) func(TaskID) []TaskID {
	return func(id TaskID) []TaskID {
		return m[id]
	}
}

func TestCheckCycle_NoCycle(t *testing.T) {
	t.Parallel()
	// A → B → C (линейная цепочка, цикла нет)
	deps := makeDeps(map[TaskID][]TaskID{
		"A": {"B"},
		"B": {"C"},
		"C": {},
	})
	if err := CheckCycle("A", deps); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestCheckCycle_SingleTask_NoDeps(t *testing.T) {
	t.Parallel()
	deps := makeDeps(map[TaskID][]TaskID{"A": {}})
	if err := CheckCycle("A", deps); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// TestCheckCycle_DirectCycle — A зависит от B, B зависит от A (МТ.2.5).
func TestCheckCycle_DirectCycle(t *testing.T) {
	t.Parallel()
	deps := makeDeps(map[TaskID][]TaskID{
		"A": {"B"},
		"B": {"A"},
	})
	err := CheckCycle("A", deps)
	if !errors.Is(err, ErrCyclicDependency) {
		t.Errorf("expected ErrCyclicDependency, got %v", err)
	}
}

func TestCheckCycle_SelfDependency(t *testing.T) {
	t.Parallel()
	deps := makeDeps(map[TaskID][]TaskID{
		"A": {"A"},
	})
	err := CheckCycle("A", deps)
	if !errors.Is(err, ErrCyclicDependency) {
		t.Errorf("expected ErrCyclicDependency for self-dep, got %v", err)
	}
}

func TestCheckCycle_IndirectCycle(t *testing.T) {
	t.Parallel()
	// A → B → C → A
	deps := makeDeps(map[TaskID][]TaskID{
		"A": {"B"},
		"B": {"C"},
		"C": {"A"},
	})
	err := CheckCycle("A", deps)
	if !errors.Is(err, ErrCyclicDependency) {
		t.Errorf("expected ErrCyclicDependency for indirect cycle, got %v", err)
	}
}

// TestCheckCycle_Diamond_NoCycle — diamond-граф (A→B, A→C, B→D, C→D) не является циклом.
func TestCheckCycle_Diamond_NoCycle(t *testing.T) {
	t.Parallel()
	deps := makeDeps(map[TaskID][]TaskID{
		"A": {"B", "C"},
		"B": {"D"},
		"C": {"D"},
		"D": {},
	})
	if err := CheckCycle("A", deps); err != nil {
		t.Errorf("diamond graph is not a cycle, got error: %v", err)
	}
}
