package storage

import "context"

// These methods reserve the planned Redis storage capabilities. They are not
// wired into commands until expiration, concurrency and persistence semantics
// have implementations and behavior tests. Keep failures explicit meanwhile.
func (*Xsync) ReadKeys(context.Context, []string, ReadKeysFunc) error     { panic("todo") }
func (*Xsync) UpdateKeys(context.Context, []string, UpdateKeysFunc) error { panic("todo") }
func (*Xsync) Scan(context.Context, Cursor, int) (EntryPage, error)       { panic("todo") }
func (*Xsync) Size(context.Context) (int64, error)                        { panic("todo") }
func (*Xsync) RandomKey(context.Context) (string, bool, error)            { panic("todo") }
func (*Xsync) Clear(context.Context, ReclaimMode) error                   { panic("todo") }
func (*Xsync) UnlinkKeys(context.Context, []string) (int64, error)        { panic("todo") }
func (*Xsync) Watch(context.Context, []string) (Watch, error)             { panic("todo") }
func (*Xsync) Atomic(context.Context, Watch, AtomicFunc) (bool, error)    { panic("todo") }
func (*Xsync) Inspect(context.Context, string) (Metadata, bool, error)    { panic("todo") }
func (*Xsync) Touch(context.Context, []string) (int64, error)             { panic("todo") }

func (*Xsync) ScanEntries(Cursor, int) EntryPage            { panic("todo") }
func (*Xsync) SampleEntries(int) []KeyEntry                 { panic("todo") }
func (*Xsync) ClearEntries(ReclaimMode)                     { panic("todo") }
func (*Xsync) Apply([]Mutation)                             { panic("todo") }
func (*Xsync) ReplaceEntries(map[string]Entry, ReclaimMode) { panic("todo") }
func (*Xsync) EntryMetadata(string) (Metadata, bool)        { panic("todo") }
func (*Xsync) TouchEntry(string, int64)                     { panic("todo") }
func (*Xsync) SetEntryMetadata(string, Metadata)            { panic("todo") }
