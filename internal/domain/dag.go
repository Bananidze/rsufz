package domain

import "fmt"

// CheckCycle сообщает об ErrCyclicDependency, если граф зависимостей,
// достижимый из startID, содержит цикл.
//
// getDeps должна возвращать непосредственные зависимости задачи.
// Для неизвестных ID она должна возвращать nil.
//
// Алгоритм: DFS с трёхцветной маркировкой (white/gray/black).
// Серый (gray) узел — на текущем стеке рекурсии; повторная встреча → цикл.
func CheckCycle(startID TaskID, getDeps func(TaskID) []TaskID) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[TaskID]int)

	var dfs func(id TaskID) error
	dfs = func(id TaskID) error {
		switch color[id] {
		case gray:
			return fmt.Errorf("%w: cycle detected at %s", ErrCyclicDependency, id)
		case black:
			return nil
		}
		color[id] = gray
		for _, dep := range getDeps(id) {
			if err := dfs(dep); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}
	return dfs(startID)
}
