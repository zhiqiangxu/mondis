package keyspace

const (
	// BasePrefix for document db
	BasePrefix = "_md_"
	// MetaPrefix for meta
	MetaPrefix = BasePrefix + "m"
	// CollectionPrefix for collection
	CollectionPrefix = BasePrefix + "c"
)

var (
	// MetaPrefixBytes for meta
	MetaPrefixBytes = []byte(MetaPrefix)
	// CollectionPrefixBytes for collection
	CollectionPrefixBytes = []byte(CollectionPrefix)
)
