// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"runtime"
	"strconv"
)

func encodeLongFilename(name string) (result string) {
	if len(name) <= 245 {
		return name
	}
	var count int
	var converted bytes.Buffer
	var update bool
	ob := []byte(name)
	for _, p := range ob {
		converted.WriteByte(p)
		switch p {
		case '/':
			count = 0
		case '\\':
			if runtime.GOOS == globalWindowsOSName {
				count = 0
			}
		default:
			count++
			if count > 245 {
				update = true
				buf := converted.Bytes()
				converted.Write(buf[len(buf)-6:])
				buf = converted.Bytes()
				copy(buf[len(buf)-12:], magicSuffix)
				count = 0
			}
		}
	}
	if update {
		result = converted.String()
		return
	}
	return name
}

// Parse bucket url queries
func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, maxkeys int, encodingType string, errCode APIErrorCode) {
	errCode = ErrNone

	if values.Get("max-keys") != "" {
		var err error
		if maxkeys, err = strconv.Atoi(values.Get("max-keys")); err != nil {
			errCode = ErrInvalidMaxKeys
			return
		}
	} else {
		maxkeys = maxObjectList
	}

	prefix = trimLeadingSlash(values.Get("prefix"))
	prefix = encodeLongFilename(prefix)
	marker = trimLeadingSlash(values.Get("marker"))
	delimiter = values.Get("delimiter")
	encodingType = values.Get("encoding-type")
	return
}

func getListBucketObjectVersionsArgs(values url.Values) (prefix, marker, delimiter string, maxkeys int, encodingType, versionIDMarker string, errCode APIErrorCode) {
	errCode = ErrNone

	if values.Get("max-keys") != "" {
		var err error
		if maxkeys, err = strconv.Atoi(values.Get("max-keys")); err != nil {
			errCode = ErrInvalidMaxKeys
			return
		}
	} else {
		maxkeys = maxObjectList
	}

	prefix = trimLeadingSlash(values.Get("prefix"))
	prefix = encodeLongFilename(prefix)
	marker = trimLeadingSlash(values.Get("key-marker"))
	delimiter = values.Get("delimiter")
	encodingType = values.Get("encoding-type")
	versionIDMarker = values.Get("version-id-marker")
	return
}

// Parse bucket url queries for ListObjects V2.
func getListObjectsV2Args(values url.Values) (prefix, token, startAfter, delimiter string, fetchOwner bool, maxkeys int, encodingType string, errCode APIErrorCode) {
	errCode = ErrNone

	// The continuation-token cannot be empty.
	if val, ok := values["continuation-token"]; ok {
		if len(val[0]) == 0 {
			errCode = ErrIncorrectContinuationToken
			return
		}
	}

	if values.Get("max-keys") != "" {
		var err error
		if maxkeys, err = strconv.Atoi(values.Get("max-keys")); err != nil {
			errCode = ErrInvalidMaxKeys
			return
		}
	} else {
		maxkeys = maxObjectList
	}

	prefix = trimLeadingSlash(values.Get("prefix"))
	prefix = encodeLongFilename(prefix)
	startAfter = trimLeadingSlash(values.Get("start-after"))
	delimiter = values.Get("delimiter")
	fetchOwner = values.Get("fetch-owner") == "true"
	encodingType = values.Get("encoding-type")

	if token = values.Get("continuation-token"); token != "" {
		decodedToken, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			errCode = ErrIncorrectContinuationToken
			return
		}
		token = string(decodedToken)
	}
	return
}

// Parse bucket url queries for ?uploads
func getBucketMultipartResources(values url.Values) (prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int, encodingType string, errCode APIErrorCode) {
	errCode = ErrNone

	if values.Get("max-uploads") != "" {
		var err error
		if maxUploads, err = strconv.Atoi(values.Get("max-uploads")); err != nil {
			errCode = ErrInvalidMaxUploads
			return
		}
	} else {
		maxUploads = maxUploadsList
	}

	prefix = trimLeadingSlash(values.Get("prefix"))
	prefix = encodeLongFilename(prefix)
	keyMarker = trimLeadingSlash(values.Get("key-marker"))
	uploadIDMarker = values.Get("upload-id-marker")
	delimiter = values.Get("delimiter")
	encodingType = values.Get("encoding-type")
	return
}

// Parse object url queries
func getObjectResources(values url.Values) (uploadID string, partNumberMarker, maxParts int, encodingType string, errCode APIErrorCode) {
	var err error
	errCode = ErrNone

	if values.Get("max-parts") != "" {
		if maxParts, err = strconv.Atoi(values.Get("max-parts")); err != nil {
			errCode = ErrInvalidMaxParts
			return
		}
	} else {
		maxParts = maxPartsList
	}

	if values.Get("part-number-marker") != "" {
		if partNumberMarker, err = strconv.Atoi(values.Get("part-number-marker")); err != nil {
			errCode = ErrInvalidPartNumberMarker
			return
		}
	}

	uploadID = values.Get("uploadId")
	encodingType = values.Get("encoding-type")
	return
}
