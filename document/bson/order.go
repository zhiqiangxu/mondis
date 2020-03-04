package bson

// FYI: https://docs.mongodb.com/manual/reference/bson-type-comparison-order/

// Order for bson types
type Order byte

const (
	// MinKeyOrder for MinKey
	MinKeyOrder Order = iota
	// NullOrder for Null
	NullOrder
	// NumberOrder for Number
	NumberOrder
	// StringOrder for String and Symbol
	StringOrder
	// ObjectOrder for Object
	ObjectOrder
	// ArrayOrder for Array
	ArrayOrder
	// BinDataOrder for BinData
	BinDataOrder
	// ObjectIDOrder for ObjectID
	ObjectIDOrder
	// BooleanOrder for Boolean
	BooleanOrder
	// DateOrder for Date
	DateOrder
	// TimestampOrder for Timestamp
	TimestampOrder
	// REOrder for regular expression
	REOrder
	// MaxKeyOrder for MaxKey
	MaxKeyOrder
)
