package version

import "testing"

func TestBuild_NonEmpty(t *testing.T) {
	t.Parallel()
	if got := Build(); got == "" {
		t.Fatalf("Build() returned empty string")
	}
}

func TestBuild_LinkerOverride(t *testing.T) {
	t.Parallel()
	saved := value
	t.Cleanup(func() { value = saved })

	value = "v1.2.3"
	if got := Build(); got != "v1.2.3" {
		t.Fatalf("Build() = %q, want %q", got, "v1.2.3")
	}
}
