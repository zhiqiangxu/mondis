package bson

import (
	"testing"

	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

// FYI: https://godoc.org/go.mongodb.org/mongo-driver/bson#hdr-Native_Go_Types

func TestTypes(t *testing.T) {
	m := bson.M{
		"key":   int32(1),
		"bool":  true,
		"subM":  bson.M{"k1": "v1"}, //embedded document unmarshals to the parent type
		"subD":  bson.M{"k1": "v1"},
		"array": bson.A{"f1", "f2"},
	}

	bytes, err := bson.Marshal(m)
	if err != nil {
		t.FailNow()
	}

	var d bson.M
	err = bson.Unmarshal(bytes, &d)
	if err != nil {
		t.FailNow()
	}

	if !reflect.DeepEqual(m, d) {
		t.FailNow()
	}

	r := bson.Raw(bytes)
	eles, err := r.Elements()
	if err != nil || len(eles) != 5 {
		t.FailNow()
	}

	// if eles[0].Key() != "key" && eles[0].Value().Type != bsontype.Int32 {
	// 	t.FailNow()
	// }
}
