package ddl

// Callback when ddl happened
type Callback struct {
	OnChanged func(err error)
}

// Options for ddl
type Options struct {
	Callback Callback
}
