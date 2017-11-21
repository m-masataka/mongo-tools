// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongoexport

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/mongodb/mongo-tools/common/bson"
	"github.com/mongodb/mongo-tools/common/bson/extjson"
	//"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/json"
)

// type for reflect code
var marshalDType = reflect.TypeOf(extjson.MarshalD{})

// CSVExportOutput is an implementation of ExportOutput that writes documents to the output in CSV format.
type CSVExportOutput struct {
	// Fields is a list of field names in the bson documents to be exported.
	// A field can also use dot-delimited modifiers to address nested structures,
	// for example "location.city" or "addresses.0".
	Fields []string

	// NumExported maintains a running total of the number of documents written.
	NumExported int64

	// NoHeaderLine, if set, will export CSV data without a list of field names at the first line
	NoHeaderLine bool

	csvWriter *csv.Writer
}

// NewCSVExportOutput returns a CSVExportOutput configured to write output to the
// given io.Writer, extracting the specified fields only.
func NewCSVExportOutput(fields []string, noHeaderLine bool, out io.Writer) *CSVExportOutput {
	return &CSVExportOutput{
		fields,
		0,
		noHeaderLine,
		csv.NewWriter(out),
	}
}

// WriteHeader writes a comma-delimited list of fields as the output header row.
func (csvExporter *CSVExportOutput) WriteHeader() error {
	if !csvExporter.NoHeaderLine {
		csvExporter.csvWriter.Write(csvExporter.Fields)
		return csvExporter.csvWriter.Error()
	}
	return nil
}

// WriteFooter is a no-op for CSV export formats.
func (csvExporter *CSVExportOutput) WriteFooter() error {
	// no CSV footer
	return nil
}

// Flush writes any pending data to the underlying I/O stream.
func (csvExporter *CSVExportOutput) Flush() error {
	csvExporter.csvWriter.Flush()
	return csvExporter.csvWriter.Error()
}

// ExportDocument writes a line to output with the CSV representation of a document.
func (csvExporter *CSVExportOutput) ExportDocument(document bson.D) error {
	fmt.Println("\n document:", document)
	rowOut := make([]string, 0, len(csvExporter.Fields))

	//extendedDoc, err := extjson.EncodeBSONDtoJSON(document)         // byte []byte
	//extendedDocBS, err := bsonutil.ConvertBSONValueToJSON(document) // type bsonutil.MarshalD
	//if err != nil {
	//	return err
	//}
	//fmt.Println("\n extendedDoc:", string(extendedDoc))
	//fmt.Println("\n extendedDocBS:", extendedDocBS)

	for _, fieldName := range csvExporter.Fields {
		fieldVal := extractFieldByName(fieldName, extjson.MarshalD(document))
		//fieldVal := extractFieldByName(fieldName, string(extendedDoc))
		fmt.Println("\n fieldVal, Name:", fieldVal, fieldName)

		if fieldVal == nil {
			rowOut = append(rowOut, "")
		} else if reflect.TypeOf(fieldVal) == reflect.TypeOf(bson.M{}) ||
			reflect.TypeOf(fieldVal) == reflect.TypeOf(bson.D{}) ||
			reflect.TypeOf(fieldVal) == marshalDType ||
			reflect.TypeOf(fieldVal) == reflect.TypeOf([]interface{}{}) {
			buf, err := json.Marshal(fieldVal)
			if err != nil {
				rowOut = append(rowOut, "")
			} else {
				rowOut = append(rowOut, string(buf))
			}
		} else {
			rowOut = append(rowOut, fmt.Sprintf("%v", fieldVal))
		}
	}

	fmt.Println("\n ROW OUT:", rowOut)
	csvExporter.csvWriter.Write(rowOut)
	csvExporter.NumExported++
	return csvExporter.csvWriter.Error()
}

// extractFieldByName takes a field name and document, and returns a value representing
// the value of that field in the document in a format that can be printed as a string.
// It will also handle dot-delimited field names for nested arrays or documents.
func extractFieldByName(fieldName string, document interface{}) interface{} {
	dotParts := strings.Split(fieldName, ".")
	var subdoc interface{} = document

	for _, path := range dotParts {

		fmt.Println("\nNew Loop")
		fmt.Println("PATH IS  ", path)
		fmt.Println("subdoc IS  ", subdoc)

		docValue := reflect.ValueOf(subdoc)
		if !docValue.IsValid() {
			return ""
		}
		docType := docValue.Type()
		docKind := docType.Kind()

		fmt.Println("docValue", docValue)
		fmt.Println("docKind", docKind)
		fmt.Println("docType", docType)
		if docKind == reflect.Map {
			fmt.Println("MAP TYPE", path)

			subdocVal := docValue.MapIndex(reflect.ValueOf(path))
			if subdocVal.Kind() == reflect.Invalid {
				return ""
			}

			subdoc = subdocVal.Interface()
			fmt.Println("SUBDOC IS ", subdoc)
		} else if docKind == reflect.Slice {
			fmt.Println("SLICE TYPE", path)
			if docType == marshalDType  || docType == reflect.TypeOf(bson.D{}) {
				var asD = bson.D{}
				if (docType == marshalDType) {
					fmt.Println("marshalDType TYPE", path)
					// dive into a D as a document
					asD = bson.D(subdoc.(extjson.MarshalD))
				} else {
					fmt.Println("bsonDType TYPE", path)
					asD = subdoc.(bson.D)
				}

				var err error
				subdoc, err = FindValueByKey(path, &asD)
				if err != nil {
					return ""
				}
				fmt.Println("SUBDOC IS ", subdoc)
				fmt.Println("FINDING PATH", path, subdoc, asD, err)
			} else {
				fmt.Println("OTHER TYPE", path)

				//  check that the path can be converted to int
				arrayIndex, err := strconv.Atoi(path)
				if err != nil {
					return ""
				}
				// bounds check for slice
				if arrayIndex < 0 || arrayIndex >= docValue.Len() {
					return ""
				}
				subdocVal := docValue.Index(arrayIndex)
				if subdocVal.Kind() == reflect.Invalid {
					return ""
				}
				fmt.Println(subdocVal.Type())
				subdoc = subdocVal.Interface()
				fmt.Println("SUBDOC IS ", subdoc)
			}
		} else {
			// trying to index into a non-compound type - just return blank.
			return ""
		}
	}
	return subdoc
}

var ErrNoSuchField = errors.New("no such field")

// FindValueByKey returns the value of keyName in document. If keyName is not found
// in the top-level of the document, ErrNoSuchField is returned as the error.
func FindValueByKey(keyName string, document *bson.D) (interface{}, error) {
	for _, key := range *document {
		if key.Name == keyName {
			return key.Value, nil
		}
	}
	return nil, ErrNoSuchField
}
