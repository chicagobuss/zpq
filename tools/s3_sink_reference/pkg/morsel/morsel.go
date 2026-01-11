package morsel

// RowGroupMeta represents the minimal metadata needed for a row group
type RowGroupMeta struct {
	NumRows      int64
	TotalByteSize int64
}

// Morsel represents a unit of work (encoded column chunks) to be uploaded
type Morsel struct {
	Data []byte
	Meta RowGroupMeta
}
