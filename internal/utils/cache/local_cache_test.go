package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocalCacheInitAndClose(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)

	err = CloseLocalCache()
	assert.NoError(t, err)
}

func TestLocalStore(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	// Test storing a string
	err = LocalStore("test_string", "testValue", 1*time.Hour)
	assert.NoError(t, err)

	// Test storing a struct
	type testStruct struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	testData := testStruct{ID: 1, Name: "test"}
	err = LocalStore("test_struct", testData, 1*time.Hour)
	assert.NoError(t, err)
}

func TestLocalGet(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	// Store and get a string
	testKey := "test_get_string"
	testValue := "testValue"

	err = LocalStore(testKey, testValue, 1*time.Hour)
	assert.NoError(t, err)

	// Wait a bit to ensure storage is complete
	time.Sleep(10 * time.Millisecond)

	val, err := LocalGetString(testKey)
	assert.NoError(t, err)
	assert.Equal(t, testValue, val)

	// Store and get a struct
	type testStruct struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	testData := testStruct{ID: 1, Name: "test"}
	structKey := "test_get_struct"
	err = LocalStore(structKey, testData, 1*time.Hour)
	assert.NoError(t, err)

	// Wait a bit to ensure storage is complete
	time.Sleep(10 * time.Millisecond)

	result, err := LocalGet[testStruct](structKey)
	if assert.NoError(t, err) && assert.NotNil(t, result) {
		assert.Equal(t, testData.ID, result.ID)
		assert.Equal(t, testData.Name, result.Name)
	}

	// Test non-existent key
	_, err = LocalGetString("non_existent_key")
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

func TestLocalDel(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	// Store and then delete
	err = LocalStore("test_del", "testValue", 1*time.Hour)
	assert.NoError(t, err)

	// Wait a bit to ensure storage is complete
	time.Sleep(10 * time.Millisecond)

	count, err := LocalDel("test_del")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Check it's gone
	_, err = LocalGetString("test_del")
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

func TestLocalExist(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	// Check non-existent key
	exist, err := LocalExist("nonexistent_key")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exist)

	// Store and check exists
	err = LocalStore("test_exist", "value", 1*time.Hour)
	assert.NoError(t, err)

	// Wait a bit to ensure storage is complete
	time.Sleep(10 * time.Millisecond)

	exist, err = LocalExist("test_exist")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), exist)
}

func TestLocalIncreaseDecrease(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	counterKey := "test_counter"

	// Test increase on new key
	val, err := LocalIncrease(counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val)

	// Test increase on existing key - reset first
	_, err = LocalDel(counterKey)
	assert.NoError(t, err)

	// Set initial value and test increase
	err = LocalStore(counterKey, int64(1), 1*time.Hour)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	val, err = LocalIncrease(counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), val)

	// Test decrease
	val, err = LocalDecrease(counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), val)

	// Test decrease below zero
	val, err = LocalDecrease(counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), val)

	val, err = LocalDecrease(counterKey)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), val)
}

func TestLocalSetExpire(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	key := "test_set_expire"

	// Store value
	err = LocalStore(key, "value", 0) // No initial expiration
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Check it exists before setting expiration
	val, err := LocalGetString(key)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// Set expiration
	err = LocalSetExpire(key, 100*time.Millisecond)
	assert.NoError(t, err)

	// Check it still exists immediately after setting expiration
	val, err = LocalGetString(key)
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be gone now
	_, err = LocalGetString(key)
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

func TestLocalSetMapField(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	// Set map fields
	mapKey := "test_map_field"
	mapData := map[string]any{
		"name": "John",
		"age":  "30", // Store as string to ensure it works with GetMapFieldString
		"city": "New York",
	}

	err = LocalSetMapField(mapKey, mapData)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Get individual fields
	name, err := LocalGetMapFieldString(mapKey, "name")
	if err != nil {
		t.Logf("Error getting 'name' field: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, "John", name)

	age, err := LocalGetMapFieldString(mapKey, "age")
	if err != nil {
		t.Logf("Error getting 'age' field: %v", err)
	}
	assert.NoError(t, err)
	assert.Equal(t, "30", age)
}

func TestLocalSetMapOneField(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	mapKey := "test_one_field_map"

	// Set one field
	err = LocalSetMapOneField(mapKey, "field1", "value1")
	assert.NoError(t, err)

	// Add another field - use string representation for number
	err = LocalSetMapOneField(mapKey, "field2", "42")
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Check both fields
	field1, err := LocalGetMapFieldString(mapKey, "field1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", field1)

	field2, err := LocalGetMapFieldString(mapKey, "field2")
	assert.NoError(t, err)
	assert.Equal(t, "42", field2)
}

func TestLocalGetMapField(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	mapKey := "test_get_map_field"

	// Store a simpler structure that's easier to JSON encode/decode
	type Person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	// Create a JSON string directly to avoid serialization issues
	jsonValue := `{"name":"Alice","age":28}`
	err = LocalSetMapOneField(mapKey, "person", jsonValue)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Retrieve it as string first to debug
	jsonString, err := LocalGetMapFieldString(mapKey, "person")
	assert.NoError(t, err)
	assert.Equal(t, jsonValue, jsonString)

	// Retrieve it as Person object
	retrievedPerson, err := LocalGetMapField[Person](mapKey, "person")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedPerson)
	assert.Equal(t, "Alice", retrievedPerson.Name)
	assert.Equal(t, 28, retrievedPerson.Age)

	// Test non-existent field
	_, err = LocalGetMapField[Person](mapKey, "nonexistent")
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

func TestLocalDelMapField(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	mapKey := "test_del_map_field"

	// Set multiple fields
	err = LocalSetMapField(mapKey, map[string]any{
		"field1": "value1",
		"field2": "value2",
	})
	assert.NoError(t, err)

	// Delete one field
	err = LocalDelMapField(mapKey, "field1")
	assert.NoError(t, err)

	// field1 should be gone
	_, err = LocalGetMapFieldString(mapKey, "field1")
	assert.ErrorIs(t, err, ErrLocalNotFound)

	// field2 should still be there
	val, err := LocalGetMapFieldString(mapKey, "field2")
	assert.NoError(t, err)
	assert.Equal(t, "value2", val)
}

func TestLocalGetMap(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	type Person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	mapKey := "test_get_map"

	// Set fields individually with JSON strings
	err = LocalSetMapOneField(mapKey, "person1", `{"name":"John","age":30}`)
	assert.NoError(t, err)

	err = LocalSetMapOneField(mapKey, "person2", `{"name":"Jane","age":25}`)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Get the entire map
	result, err := LocalGetMap[Person](mapKey)
	assert.NoError(t, err)

	// Check the map has expected values
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "John", result["person1"].Name)
	assert.Equal(t, 30, result["person1"].Age)
	assert.Equal(t, "Jane", result["person2"].Name)
	assert.Equal(t, 25, result["person2"].Age)
}

func TestLocalScanMap(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	mapKey := "test_scan_map"

	// Use direct JSON strings for data
	err = LocalSetMapOneField(mapKey, "key1", `{"field":"value1"}`)
	assert.NoError(t, err)

	err = LocalSetMapOneField(mapKey, "key2", `{"field":"value2"}`)
	assert.NoError(t, err)

	err = LocalSetMapOneField(mapKey, "key3", `{"field":"value3"}`)
	assert.NoError(t, err)

	err = LocalSetMapOneField(mapKey, "4", `{"field":"value4"}`)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Define struct for JSON unmarshaling
	type TestData struct {
		Field string `json:"field"`
	}

	// ScanMap should return all fields since Ristretto doesn't do pattern matching
	data, err := LocalScanMap[TestData](mapKey, "key*")
	assert.NoError(t, err)

	// Since local cache doesn't support pattern matching, we'll get all entries
	assert.GreaterOrEqual(t, len(data), 3, "Should have at least 3 entries")

	// Check that the entries contain the expected values
	keyCount := 0
	for key, val := range data {
		if key == "key1" && val.Field == "value1" {
			keyCount++
		}
		if key == "key2" && val.Field == "value2" {
			keyCount++
		}
		if key == "key3" && val.Field == "value3" {
			keyCount++
		}
	}
	assert.GreaterOrEqual(t, keyCount, 3, "Should find at least key1, key2, and key3")

	// Test ScanMapAsync
	callCount := 0
	err = LocalScanMapAsync[TestData](mapKey, "*", func(m map[string]TestData) error {
		callCount++
		assert.GreaterOrEqual(t, len(m), 1, "Map should have at least 1 entry")
		return nil
	})
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 1, "Callback should have been called at least once")
}

func TestLocalSetNX(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	key := "test_setnx"

	// Set a new key
	success, err := LocalSetNX(key, "original_value", 1*time.Hour)
	assert.NoError(t, err)
	assert.True(t, success, "First SetNX should succeed")

	// Try to set it again
	success, err = LocalSetNX(key, "new_value", 1*time.Hour)
	assert.NoError(t, err)
	assert.False(t, success, "Second SetNX should fail")

	// Check the value is still the original
	val, err := LocalGetString(key)
	assert.NoError(t, err)
	assert.Equal(t, "original_value", val)
}

func TestLocalLockUnlock(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	lockKey := "test_lock"

	// Acquire a lock
	err = LocalLock(lockKey, 1*time.Second, 100*time.Millisecond)
	assert.NoError(t, err)

	// Try to acquire the same lock, should timeout
	err = LocalLock(lockKey, 1*time.Second, 100*time.Millisecond)
	assert.ErrorIs(t, err, ErrLocalLockTimeout)

	// Unlock
	err = LocalUnlock(lockKey)
	assert.NoError(t, err)

	// Should be able to acquire again
	err = LocalLock(lockKey, 1*time.Second, 100*time.Millisecond)
	assert.NoError(t, err)
}

func TestLocalExpire(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	key := "test_expire"

	// Store a value without expiration
	err = LocalStore(key, "value", 0)
	assert.NoError(t, err)

	// Set expiration
	success, err := LocalExpire(key, 100*time.Millisecond)
	assert.NoError(t, err)
	assert.True(t, success)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Value should be gone
	_, err = LocalGetString(key)
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

func TestLocalTransaction(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	key := "test_transaction"

	// LocalTransaction is just a placeholder that executes the function
	err = LocalTransaction(func() error {
		return LocalStore(key, "transactional_value", 1*time.Hour)
	})
	assert.NoError(t, err)

	// Check the value was stored
	val, err := LocalGetString(key)
	assert.NoError(t, err)
	assert.Equal(t, "transactional_value", val)
}

func TestLocalPublishSubscribe(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	channel := "test_pubsub"

	type TestMessage struct {
		Text string `json:"text"`
		ID   int    `json:"id"`
	}

	// Create a channel and subscribe
	msgCh, unsubscribe := LocalSubscribe[TestMessage](channel)
	defer unsubscribe()

	// Start a goroutine to collect received messages
	received := make([]TestMessage, 0)
	done := make(chan bool)

	go func() {
		count := 0
		for msg := range msgCh {
			received = append(received, msg)
			count++
			if count >= 3 {
				done <- true
				break
			}
		}
	}()

	// Publish messages with a small delay
	for i := 1; i <= 3; i++ {
		err := LocalPublish(channel, TestMessage{
			Text: fmt.Sprintf("Message %d", i),
			ID:   i,
		})
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all messages or timeout
	select {
	case <-done:
		// All messages received
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed out waiting for messages")
	}

	// Check received messages
	assert.Equal(t, 3, len(received))
	assert.Equal(t, "Message 1", received[0].Text)
	assert.Equal(t, 1, received[0].ID)
	assert.Equal(t, "Message 2", received[1].Text)
	assert.Equal(t, 2, received[1].ID)
	assert.Equal(t, "Message 3", received[2].Text)
	assert.Equal(t, 3, received[2].ID)
}

func TestCacheExpiration(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	key := "test_expiration"

	// Store with short expiration
	err = LocalStore(key, "temporary_value", 100*time.Millisecond)
	assert.NoError(t, err)

	// Check it exists
	val, err := LocalGetString(key)
	assert.NoError(t, err)
	assert.Equal(t, "temporary_value", val)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be gone now
	_, err = LocalGetString(key)
	assert.ErrorIs(t, err, ErrLocalNotFound)
}

// Test the concurrent access pattern
func TestConcurrentAccess(t *testing.T) {
	err := InitLocalCache()
	assert.NoError(t, err)
	defer CloseLocalCache()

	const goroutineCount = 100
	const operationsPerGoroutine = 10

	wg := sync.WaitGroup{}
	wg.Add(goroutineCount)

	// Launch goroutines to perform operations
	for i := 0; i < goroutineCount; i++ {
		go func(routineID int) {
			defer wg.Done()

			key := fmt.Sprintf("concurrent_key_%d", routineID)

			// Perform multiple operations
			for j := 0; j < operationsPerGoroutine; j++ {
				// Store
				err := LocalStore(key, fmt.Sprintf("value_%d_%d", routineID, j), 1*time.Second)
				assert.NoError(t, err)

				// Get
				_, err = LocalGetString(key)
				if err != nil && err != ErrLocalNotFound {
					t.Errorf("Unexpected error: %v", err)
				}

				// Increment counter
				counterKey := fmt.Sprintf("concurrent_counter_%d", routineID)
				_, err = LocalIncrease(counterKey)
				assert.NoError(t, err)
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
