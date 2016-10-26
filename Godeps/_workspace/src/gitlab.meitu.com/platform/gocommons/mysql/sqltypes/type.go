// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

// Type defines the various supported data types in bind vars
// and query results.
type Type int32

const (
	// NULL_TYPE specifies a NULL type.
	Type_NULL_TYPE Type = 0
	// INT8 specifies a TINYINT type.
	// Properties: 1, IsNumber.
	Type_INT8 Type = 257
	// UINT8 specifies a TINYINT UNSIGNED type.
	// Properties: 2, IsNumber, IsUnsigned.
	Type_UINT8 Type = 770
	// INT16 specifies a SMALLINT type.
	// Properties: 3, IsNumber.
	Type_INT16 Type = 259
	// UINT16 specifies a SMALLINT UNSIGNED type.
	// Properties: 4, IsNumber, IsUnsigned.
	Type_UINT16 Type = 772
	// INT24 specifies a MEDIUMINT type.
	// Properties: 5, IsNumber.
	Type_INT24 Type = 261
	// UINT24 specifies a MEDIUMINT UNSIGNED type.
	// Properties: 6, IsNumber, IsUnsigned.
	Type_UINT24 Type = 774
	// INT32 specifies a INTEGER type.
	// Properties: 7, IsNumber.
	Type_INT32 Type = 263
	// UINT32 specifies a INTEGER UNSIGNED type.
	// Properties: 8, IsNumber, IsUnsigned.
	Type_UINT32 Type = 776
	// INT64 specifies a BIGINT type.
	// Properties: 9, IsNumber.
	Type_INT64 Type = 265
	// UINT64 specifies a BIGINT UNSIGNED type.
	// Properties: 10, IsNumber, IsUnsigned.
	Type_UINT64 Type = 778
	// FLOAT32 specifies a FLOAT type.
	// Properties: 11, IsFloat.
	Type_FLOAT32 Type = 1035
	// FLOAT64 specifies a DOUBLE or REAL type.
	// Properties: 12, IsFloat.
	Type_FLOAT64 Type = 1036
	// TIMESTAMP specifies a TIMESTAMP type.
	// Properties: 13, IsQuoted.
	Type_TIMESTAMP Type = 2061
	// DATE specifies a DATE type.
	// Properties: 14, IsQuoted.
	Type_DATE Type = 2062
	// TIME specifies a TIME type.
	// Properties: 15, IsQuoted.
	Type_TIME Type = 2063
	// DATETIME specifies a DATETIME type.
	// Properties: 16, IsQuoted.
	Type_DATETIME Type = 2064
	// YEAR specifies a YEAR type.
	// Properties: 17, IsNumber, IsUnsigned.
	Type_YEAR Type = 785
	// DECIMAL specifies a DECIMAL or NUMERIC type.
	// Properties: 18, None.
	Type_DECIMAL Type = 18
	// TEXT specifies a TEXT type.
	// Properties: 19, IsQuoted, IsText.
	Type_TEXT Type = 6163
	// BLOB specifies a BLOB type.
	// Properties: 20, IsQuoted, IsBinary.
	Type_BLOB Type = 10260
	// VARCHAR specifies a VARCHAR type.
	// Properties: 21, IsQuoted, IsText.
	Type_VARCHAR Type = 6165
	// VARBINARY specifies a VARBINARY type.
	// Properties: 22, IsQuoted, IsBinary.
	Type_VARBINARY Type = 10262
	// CHAR specifies a CHAR type.
	// Properties: 23, IsQuoted, IsText.
	Type_CHAR Type = 6167
	// BINARY specifies a BINARY type.
	// Properties: 24, IsQuoted, IsBinary.
	Type_BINARY Type = 10264
	// BIT specifies a BIT type.
	// Properties: 25, IsQuoted.
	Type_BIT Type = 2073
	// ENUM specifies an ENUM type.
	// Properties: 26, IsQuoted.
	Type_ENUM Type = 2074
	// SET specifies a SET type.
	// Properties: 27, IsQuoted.
	Type_SET Type = 2075
	// TUPLE specifies a a tuple. This cannot
	// be returned in a QueryResult, but it can
	// be sent as a bind var.
	// Properties: 28, None.
	Type_TUPLE Type = 28
)

// These bit flags can be used to query on the
// common properties of types.
const (
	flagIsIntegral = 256
	flagIsUnsigned = 512
	flagIsFloat    = 1024
	flagIsQuoted   = 2048
	flagIsText     = 4096
	flagIsBinary   = 8192
)

// IsIntegral returns true if Type is an integral
// (signed/unsigned) that can be represented using
// up to 64 binary bits.
func IsIntegral(t Type) bool {
	return int(t)&flagIsIntegral == flagIsIntegral
}

// IsSigned returns true if Type is a signed integral.
func IsSigned(t Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral
}

// IsUnsigned returns true if Type is an unsigned integral.
// Caution: this is not the same as !IsSigned.
func IsUnsigned(t Type) bool {
	return int(t)&(flagIsIntegral|flagIsUnsigned) == flagIsIntegral|flagIsUnsigned
}

// IsFloat returns true is Type is a floating point.
func IsFloat(t Type) bool {
	return int(t)&flagIsFloat == flagIsFloat
}

// IsQuoted returns true if Type is a quoted text or binary.
func IsQuoted(t Type) bool {
	return int(t)&flagIsQuoted == flagIsQuoted
}

// IsText returns true if Type is a text.
func IsText(t Type) bool {
	return int(t)&flagIsText == flagIsText
}

// IsBinary returns true if Type is a binary.
func IsBinary(t Type) bool {
	return int(t)&flagIsBinary == flagIsBinary
}

// These are idiomatically named synonyms for the Type values.
const (
	Null      = Type_NULL_TYPE
	Int8      = Type_INT8
	Uint8     = Type_UINT8
	Int16     = Type_INT16
	Uint16    = Type_UINT16
	Int24     = Type_INT24
	Uint24    = Type_UINT24
	Int32     = Type_INT32
	Uint32    = Type_UINT32
	Int64     = Type_INT64
	Uint64    = Type_UINT64
	Float32   = Type_FLOAT32
	Float64   = Type_FLOAT64
	Timestamp = Type_TIMESTAMP
	Date      = Type_DATE
	Time      = Type_TIME
	Datetime  = Type_DATETIME
	Year      = Type_YEAR
	Decimal   = Type_DECIMAL
	Text      = Type_TEXT
	Blob      = Type_BLOB
	VarChar   = Type_VARCHAR
	VarBinary = Type_VARBINARY
	Char      = Type_CHAR
	Binary    = Type_BINARY
	Bit       = Type_BIT
	Enum      = Type_ENUM
	Set       = Type_SET
	Tuple     = Type_TUPLE
)
