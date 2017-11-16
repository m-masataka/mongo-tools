// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongoexport

import (
	"bytes"

	"github.com/mongodb/mongo-tools/common/json"
	"github.com/mongodb/mongo-tools/common/testutil"
	. "github.com/smartystreets/goconvey/convey"
	//"gopkg.in/mgo.v2/bson"
	"fmt"
	"testing"

	"github.com/mongodb/mongo-tools/common/bson"
)

func TestWriteJSON(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	Convey("With a JSON export output", t, func() {
		out := &bytes.Buffer{}

		Convey("Special types should serialize as extended JSON", func() {

			Convey("ObjectId should have an extended JSON format", func() {
				jsonExporter := NewJSONExportOutput(false, false, out)
				objId := bson.NewObjectId()
				err := jsonExporter.WriteHeader()
				So(err, ShouldBeNil)
				err = jsonExporter.ExportDocument(bson.D{{"_id", objId}})
				So(err, ShouldBeNil)
				//err = jsonExporter.ExportDocument(bson.D{{"bnry", bson.Binary{0x00, []byte("sdfas")}}})
				//So(err, ShouldBeNil)
				err = jsonExporter.WriteFooter()
				So(err, ShouldBeNil)
				fmt.Println("TestWriteJSON", out.String())
				So(out.String(), ShouldEqual, `{"_id":{"$oid":"`+objId.Hex()+`"}}`+"\n")
			})

			Reset(func() {
				out.Reset()
			})
		})

	})
}

func TestJSONArray(t *testing.T) {
	testutil.VerifyTestType(t, testutil.UnitTestType)

	Convey("With a JSON export output in array mode", t, func() {
		out := &bytes.Buffer{}
		Convey("exporting a bunch of documents should produce valid json", func() {
			jsonExporter := NewJSONExportOutput(true, false, out)
			err := jsonExporter.WriteHeader()
			So(err, ShouldBeNil)

			// Export a few docs of various types
			//
			////out.Bytes() ["{\"_id\":{\"$oid\":\"5a0cce3602bf894044078833\"}}","{\"_id\":\"asd\"}","{\"_id\":{\"$numberInt\":\"12345\"}}","{\"_id\":{\"$binary\":{\"base64\":\"c2RmYXM=\",\"subType\":\"00\"}}}","{\"_id\":{\"$numberDouble\":\"3.14159\"}}","{\"_id\":{\"A\":{\"$numberInt\":\"1\"}}}"]
			////out.Bytes() [{"_id":{"$oid":"5a0cce6002bf8940724fe6cc"}},{"_id":"asd"},{"_id":12345},{"_id":{"$binary":"c2RmYXM=","$type":"00"}},{"_id":3.14159},{"_id":{"A":1}}]
			//
			//([^\\])\"   \1
			//\\\"       \"

			//[{"_id":{"$oid":"5a0cce6002bf8940724fe6cc"}},{"_id":"asd"},{"_id":12345},{"_id":{"$binary":"c2RmYXM=","$type":"00"}},{"_id":3.14159},{"_id":{"A":1}}]

			//out.Bytes() ["{\"_id\":{\"$oid\":\"5a0cce3602bf894044078833\"}}",
			// 					"{\"_id\":\"asd\"}",
			// 					"{\"_id\":{\"$numberInt\":\"12345\"}}",
			// 					"{\"_id\":{\"$binary\":{\"base64\":\"c2RmYXM=\",\"subType\":\"00\"}}}",
			// 					"{\"_id\":{\"$numberDouble\":\"3.14159\"}}",
			// 					"{\"_id\":{\"A\":{\"$numberInt\":\"1\"}}}"
			// 				]

			testObjs := []interface{}{bson.NewObjectId(), "asd", 12345, bson.Binary{0x00, []byte("sdfas")}, 3.14159, bson.D{{"A", 1}}}
			for _, obj := range testObjs {
				err = jsonExporter.ExportDocument(bson.D{{"_id", obj}})
				So(err, ShouldBeNil)
			}

			err = jsonExporter.WriteFooter()
			So(err, ShouldBeNil)
			// Unmarshal the whole thing, it should be valid json
			fromJSON := []map[string]interface{}{}

			strOut := string(out.Bytes())
			fmt.Println("\n out.Bytes()", strOut)

			err = json.Unmarshal(out.Bytes(), &fromJSON)
			fmt.Println("\n fromJSON err", fromJSON, err)

			So(err, ShouldBeNil)
			So(len(fromJSON), ShouldEqual, len(testObjs))

		})

		Reset(func() {
			out.Reset()
		})

	})
}
