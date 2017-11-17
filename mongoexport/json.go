// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongoexport

import (
	"bytes"
	"fmt"
	"io"

	"strconv"

	"github.com/mongodb/mongo-tools/common/bson"
	"github.com/mongodb/mongo-tools/common/bson/extjson"
	"github.com/mongodb/mongo-tools/common/json"
)

// JSONExportOutput is an implementation of ExportOutput that writes documents
// to the output in JSON format.
type JSONExportOutput struct {
	// ArrayOutput when set to true indicates that the output should be written
	// as a JSON array, where each document is an element in the array.
	ArrayOutput bool
	// Pretty when set to true indicates that the output will be written in pretty mode.
	PrettyOutput bool
	Encoder      *json.Encoder
	Out          io.Writer
	NumExported  int64
}

// NewJSONExportOutput creates a new JSONExportOutput in array mode if specified,
// configured to write data to the given io.Writer.
func NewJSONExportOutput(arrayOutput bool, prettyOutput bool, out io.Writer) *JSONExportOutput {
	return &JSONExportOutput{
		arrayOutput,
		prettyOutput,
		json.NewEncoder(out),
		out,
		0,
	}
}

// WriteHeader writes the opening square bracket if in array mode, otherwise it
// behaves as a no-op.
func (jsonExporter *JSONExportOutput) WriteHeader() error {
	if jsonExporter.ArrayOutput {
		// TODO check # bytes written?
		_, err := jsonExporter.Out.Write([]byte{json.ArrayStart})
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteFooter writes the closing square bracket if in array mode, otherwise it
// behaves as a no-op.
func (jsonExporter *JSONExportOutput) WriteFooter() error {
	if jsonExporter.ArrayOutput {
		_, err := jsonExporter.Out.Write([]byte{json.ArrayEnd, '\n'})
		// TODO check # bytes written?
		if err != nil {
			return err
		}
	}
	if jsonExporter.PrettyOutput {
		jsonExporter.Out.Write([]byte("\n"))
	}
	return nil
}

// Flush is a no-op for JSON export formats.
func (jsonExporter *JSONExportOutput) Flush() error {
	return nil
}

// ExportDocument converts the given document to extended JSON, and writes it
// to the output.
func (jsonExporter *JSONExportOutput) ExportDocument(document bson.D) error {
	fmt.Println("json.go, ExportDocument", document)
	if jsonExporter.ArrayOutput || jsonExporter.PrettyOutput {
		if jsonExporter.NumExported >= 1 {
			if jsonExporter.ArrayOutput {
				jsonExporter.Out.Write([]byte(","))
			}
			if jsonExporter.PrettyOutput {
				jsonExporter.Out.Write([]byte("\n"))
			}
		}

		//extendedDoc, err := bsonutil.ConvertBSONValueToJSON(document)
		extendedDoc, err := extjson.EncodeBSONDtoJSON(document)
		if err != nil {
			return err
		}
		//jsonOut, err := json.Marshal(extendedDoc)
		jsonOut, err := json.Marshal(string(extendedDoc))
		if err != nil {
			return fmt.Errorf("error converting BSON to extended JSON: %v", err)
		}
		if jsonExporter.PrettyOutput {
			var jsonFormatted bytes.Buffer
			json.Indent(&jsonFormatted, jsonOut, "", "\t")
			jsonOut = jsonFormatted.Bytes()
		}
		// Clean my jsonout here
		fmt.Println("JSONOUTOUTOU", string(jsonOut))
		unq, err := strconv.Unquote(string(jsonOut))
		jsonExporter.Out.Write([]byte(unq))
	} else {
		fmt.Println("SDFGHJKLKJHGFDS")
		extendedDoc, err := extjson.EncodeBSONDtoJSON(document)
		if err != nil {
			return err
		}
		fmt.Println(extendedDoc, string(extendedDoc))
		jsonExporter.Out.Write(extendedDoc)
		jsonExporter.Out.Write([]byte("\n"))

		//err = jsonExporter.Encoder.Encode(string(extendedDoc))
		if err != nil {
			return err
		}
	}
	jsonExporter.NumExported++
	return nil
}

/*
91
 out.Bytes() ["{\"_id\":{\"$oid\":\"5a0cab4202bf8932fe626c02\"}}","{\"_id\":\"asd\"}","{\"_id\":{\"$numberInt\":\"12345\"}}","{\"_id\":{\"$numberDouble\":\"3.14159\"}}","{\"_id\":{\"A\":{\"$numberInt\":\"1\"}}}"]
 fromJSON err [map[] map[] map[] map[] map[]] json: cannot unmarshal string into Go value of type map[string]interface {}

// Below works
92
 out.Bytes() [{"_id":{"$oid":"5a0cab5a02bf893329138336"}},{"_id":"asd"},{"_id":12345},{"_id":3.14159},{"_id":{"A":1}}]
 fromJSON err [map[_id:map[$oid:5a0cab5a02bf893329138336]] map[_id:asd] map[_id:12345] map[_id:3.14159] map[_id:map[A:1]]] <nil>
*/
