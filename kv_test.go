// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"testing"
	"time"
)

func expectOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestKeyValuePurgeDeletesMarkerThreshold(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	// Override the marker threshold
	kv.(*kvs).pdthr = 100 * time.Millisecond

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	put("foo", "foo1")
	put("bar", "bar1")
	put("foo", "foo2")
	err = kv.Delete("foo")
	expectOk(t, err)

	time.Sleep(200 * time.Millisecond)

	err = kv.Delete("bar")
	expectOk(t, err)

	err = kv.PurgeDeletes()
	expectOk(t, err)

	// The key foo should have been completely cleared of the data
	// and the delete marker.
	fooEntries, err := kv.History("foo")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected all entries for key foo to be gone, got err=%v entries=%v", err, fooEntries)
	}
	barEntries, err := kv.History("bar")
	expectOk(t, err)
	if len(barEntries) != 1 {
		t.Fatalf("Expected 1 entry, got %v", barEntries)
	}
	if e := barEntries[0]; e.Operation() != KeyValueDelete {
		t.Fatalf("Unexpected entry: %+v", e)
	}
}
