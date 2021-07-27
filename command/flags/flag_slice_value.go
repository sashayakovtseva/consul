package flags

import (
	"flag"
	"strings"
)

// AppendSliceValue implements the flag.Value interface and allows multiple
// calls to the same variable to append a list.
type AppendSliceValue []string

func (s *AppendSliceValue) String() string {
	return strings.Join(*s, ",")
}

func (s *AppendSliceValue) Set(value string) error {
	if *s == nil {
		*s = make([]string, 0, 1)
	}

	*s = append(*s, value)
	return nil
}

var _ flag.Value = (*CommaSliceValue)(nil)

// CommaSliceValue implements the flag.Value interface and allows comma
// separated flags to be expanded into a slice.
type CommaSliceValue []string

func (s *CommaSliceValue) String() string {
	return strings.Join(*s, ",")
}

func (s *CommaSliceValue) Set(value string) error {
	vv := strings.Split(value, ",")
	for i := range vv {
		vv[i] = strings.TrimSpace(vv[i])
	}

	*s = vv
	return nil
}
