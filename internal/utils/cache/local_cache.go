package cache

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/parser"
)

var (
	localClient *LocalCache

	ErrLocalDBNotInit = errors.New("local cache not initialized")
	ErrLocalNotFound  = errors.New("key not found in local cache")
)

// LocalCache is a local cache implementation using Ristretto
type LocalCache struct {
	cache          *ristretto.Cache[string, interface{}]
	pubsubChannels map[string]map[chan interface{}]bool
	pubsubMutex    sync.RWMutex
	keyExpires     sync.Map // tracks expiration time
}

// InitLocalCache initializes the local cache
func InitLocalCache() error {
	cache, err := ristretto.NewCache[string, interface{}](&ristretto.Config[string, interface{}]{
		// Set these values to sensible defaults for your use case
		NumCounters: 1e7,     // number of keys to track frequency of (10M)
		MaxCost:     1 << 30, // maximum cost of cache (1GB)
		BufferItems: 64,      // number of keys per Get buffer
	})
	if err != nil {
		return err
	}

	localClient = &LocalCache{
		cache:          cache,
		pubsubChannels: make(map[string]map[chan interface{}]bool),
	}

	// Start a goroutine to clean up expired keys
	go localClient.cleanupExpiredKeys()

	return nil
}

// cleanupExpiredKeys periodically checks for expired keys and removes them
func (lc *LocalCache) cleanupExpiredKeys() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixNano()
		lc.keyExpires.Range(func(k, v interface{}) bool {
			expireTime := v.(int64)
			if now > expireTime {
				key := k.(string)
				lc.cache.Del(key)
				lc.keyExpires.Delete(key)
			}
			return true
		})
	}
}

// CloseLocalCache closes the local cache
func CloseLocalCache() error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	// Close doesn't actually do anything in Ristretto, but included for API compatibility
	localClient.cache.Close()
	localClient = nil
	return nil
}

func localSerialKey(keys ...string) string {
	return strings.Join(append(
		[]string{"plugin_daemon"},
		keys...,
	), ":")
}

// LocalStore stores a key-value pair in the local cache
func LocalStore(key string, value any, expiration time.Duration) error {
	return localStore(localSerialKey(key), value, expiration)
}

// localStore stores a key-value pair in the local cache without serialKey
func localStore(key string, value any, expiration time.Duration) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	if _, ok := value.(string); !ok {
		var err error
		value, err = parser.MarshalCBOR(value)
		if err != nil {
			return err
		}
	}

	// Store expiration time if set
	if expiration > 0 {
		expireAt := time.Now().Add(expiration).UnixNano()
		localClient.keyExpires.Store(key, expireAt)
	}

	// The cost is set to 1 by default, adjust if needed
	localClient.cache.Set(key, value, 1)
	return nil
}

// LocalGet retrieves a value from the local cache
func LocalGet[T any](key string) (*T, error) {
	return localGet[T](localSerialKey(key))
}

func localGet[T any](key string) (*T, error) {
	if localClient == nil {
		return nil, ErrLocalDBNotInit
	}

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(key); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(key)
			localClient.keyExpires.Delete(key)
			return nil, ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(key)
	if !found {
		return nil, ErrLocalNotFound
	}

	valBytes, ok := val.([]byte)
	if !ok {
		// Try to convert string to bytes
		valStr, isStr := val.(string)
		if isStr {
			valBytes = []byte(valStr)
		} else {
			return nil, errors.New("invalid value type in cache")
		}
	}

	result, err := parser.UnmarshalCBOR[T](valBytes)
	return &result, err
}

// LocalGetString retrieves a string value from the local cache
func LocalGetString(key string) (string, error) {
	if localClient == nil {
		return "", ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return "", ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)
	if !found {
		return "", ErrLocalNotFound
	}

	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", errors.New("value is not a string")
	}
}

// LocalDel deletes a key from the local cache
func LocalDel(key string) (int64, error) {
	return localDel(localSerialKey(key))
}

func localDel(key string) (int64, error) {
	if localClient == nil {
		return 0, ErrLocalDBNotInit
	}

	localClient.keyExpires.Delete(key)
	localClient.cache.Del(key)
	return 1, nil
}

// LocalExist checks if a key exists in the local cache
func LocalExist(key string) (int64, error) {
	if localClient == nil {
		return 0, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return 0, nil
		}
	}

	_, found := localClient.cache.Get(serializedKey)
	if found {
		return 1, nil
	}
	return 0, nil
}

// LocalIncrease increments a numeric key value in the local cache
func LocalIncrease(key string) (int64, error) {
	if localClient == nil {
		return 0, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return 0, ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)

	var numVal int64 = 0
	if found {
		switch v := val.(type) {
		case int:
			numVal = int64(v)
		case int64:
			numVal = v
		case string:
			// Try to parse string to int64
			var err error
			numVal, err = parser.UnmarshalJson[int64](v)
			if err != nil {
				return 0, err
			}
		case []byte:
			// Try to parse []byte to int64
			var err error
			numVal, err = parser.UnmarshalJson[int64](string(v))
			if err != nil {
				return 0, err
			}
		default:
			return 0, errors.New("value is not a number")
		}
	}

	// Increment the value
	numVal++

	// Store the value back
	localClient.cache.Set(serializedKey, numVal, 1)
	return numVal, nil
}

// LocalDecrease decrements a numeric key value in the local cache
func LocalDecrease(key string) (int64, error) {
	if localClient == nil {
		return 0, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return 0, ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)

	var numVal int64 = 0
	if found {
		switch v := val.(type) {
		case int:
			numVal = int64(v)
		case int64:
			numVal = v
		case string:
			// Try to parse string to int64
			var err error
			numVal, err = parser.UnmarshalJson[int64](v)
			if err != nil {
				return 0, err
			}
		case []byte:
			// Try to parse []byte to int64
			var err error
			numVal, err = parser.UnmarshalJson[int64](string(v))
			if err != nil {
				return 0, err
			}
		default:
			return 0, errors.New("value is not a number")
		}
	}

	// Decrement the value
	numVal--

	// Store the value back
	localClient.cache.Set(serializedKey, numVal, 1)
	return numVal, nil
}

// LocalSetExpire sets the expiration time for a key
func LocalSetExpire(key string, duration time.Duration) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key exists
	_, found := localClient.cache.Get(serializedKey)
	if !found {
		return ErrLocalNotFound
	}

	// Set expiration time
	expireAt := time.Now().Add(duration).UnixNano()
	localClient.keyExpires.Store(serializedKey, expireAt)
	return nil
}

// MapValue represents a map structure in the local cache
type MapValue struct {
	sync.RWMutex
	items map[string]interface{}
}

// LocalSetMapField sets a field in a map stored in the local cache
func LocalSetMapField(key string, v map[string]any) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key already exists
	val, found := localClient.cache.Get(serializedKey)
	var mapVal *MapValue

	if found {
		mapValPtr, ok := val.(*MapValue)
		if !ok {
			// Replace existing value if it's not a MapValue
			mapVal = &MapValue{
				items: make(map[string]interface{}),
			}
		} else {
			mapVal = mapValPtr
		}
	} else {
		mapVal = &MapValue{
			items: make(map[string]interface{}),
		}
	}

	// Update the map
	mapVal.Lock()
	for field, value := range v {
		mapVal.items[field] = value
	}
	mapVal.Unlock()

	// Store the map back
	localClient.cache.Set(serializedKey, mapVal, 1)
	return nil
}

// LocalSetMapOneField sets one field in a map stored in the local cache
func LocalSetMapOneField(key string, field string, value any) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key already exists
	val, found := localClient.cache.Get(serializedKey)
	var mapVal *MapValue

	if found {
		mapValPtr, ok := val.(*MapValue)
		if !ok {
			// Replace existing value if it's not a MapValue
			mapVal = &MapValue{
				items: make(map[string]interface{}),
			}
		} else {
			mapVal = mapValPtr
		}
	} else {
		mapVal = &MapValue{
			items: make(map[string]interface{}),
		}
	}

	// Convert value to string if it's not already
	if _, ok := value.(string); !ok {
		value = parser.MarshalJson(value)
	}

	// Update the field
	mapVal.Lock()
	mapVal.items[field] = value
	mapVal.Unlock()

	// Store the map back
	localClient.cache.Set(serializedKey, mapVal, 1)
	return nil
}

// LocalGetMapField retrieves a field from a map stored in the local cache
func LocalGetMapField[T any](key string, field string) (*T, error) {
	if localClient == nil {
		return nil, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return nil, ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)
	if !found {
		return nil, ErrLocalNotFound
	}

	mapValPtr, ok := val.(*MapValue)
	if !ok {
		return nil, errors.New("value is not a map")
	}

	mapValPtr.RLock()
	fieldVal, exists := mapValPtr.items[field]
	mapValPtr.RUnlock()

	if !exists {
		return nil, ErrLocalNotFound
	}

	// Convert the field value to the requested type
	fieldStr, ok := fieldVal.(string)
	if !ok {
		return nil, errors.New("field value is not a string")
	}

	result, err := parser.UnmarshalJson[T](fieldStr)
	return &result, err
}

// LocalGetMapFieldString retrieves a string field from a map stored in the local cache
func LocalGetMapFieldString(key string, field string) (string, error) {
	if localClient == nil {
		return "", ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return "", ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)
	if !found {
		return "", ErrLocalNotFound
	}

	mapValPtr, ok := val.(*MapValue)
	if !ok {
		return "", errors.New("value is not a map")
	}

	mapValPtr.RLock()
	fieldVal, exists := mapValPtr.items[field]
	mapValPtr.RUnlock()

	if !exists {
		return "", ErrLocalNotFound
	}

	// Convert the field value to string
	switch v := fieldVal.(type) {
	case string:
		return v, nil
	default:
		return "", errors.New("field value is not a string")
	}
}

// LocalDelMapField deletes a field from a map stored in the local cache
func LocalDelMapField(key string, field string) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)
	if !found {
		return ErrLocalNotFound
	}

	mapValPtr, ok := val.(*MapValue)
	if !ok {
		return errors.New("value is not a map")
	}

	mapValPtr.Lock()
	delete(mapValPtr.items, field)
	mapValPtr.Unlock()

	return nil
}

// LocalGetMap retrieves a map from the local cache
func LocalGetMap[V any](key string) (map[string]V, error) {
	if localClient == nil {
		return nil, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key has expired
	if expireTimeIface, exists := localClient.keyExpires.Load(serializedKey); exists {
		expireTime := expireTimeIface.(int64)
		if time.Now().UnixNano() > expireTime {
			localClient.cache.Del(serializedKey)
			localClient.keyExpires.Delete(serializedKey)
			return nil, ErrLocalNotFound
		}
	}

	val, found := localClient.cache.Get(serializedKey)
	if !found {
		return nil, ErrLocalNotFound
	}

	mapValPtr, ok := val.(*MapValue)
	if !ok {
		return nil, errors.New("value is not a map")
	}

	result := make(map[string]V)

	mapValPtr.RLock()
	for k, v := range mapValPtr.items {
		strV, ok := v.(string)
		if !ok {
			continue
		}

		value, err := parser.UnmarshalJson[V](strV)
		if err != nil {
			continue
		}

		result[k] = value
	}
	mapValPtr.RUnlock()

	return result, nil
}

// LocalScanKeys is a placeholder for Redis SCAN operation
// Since Ristretto doesn't have a built-in way to scan keys, we implement a best-effort approach
func LocalScanKeys(match string) ([]string, error) {
	if localClient == nil {
		return nil, ErrLocalDBNotInit
	}

	// This is not an efficient implementation, but it's a placeholder
	// In a real-world scenario, you might want to implement a proper key indexing mechanism
	log.Warn("LocalScanKeys is not efficiently implemented for Ristretto")
	return []string{}, nil
}

// LocalScanKeysAsync is a placeholder for Redis SCAN operation with callback
func LocalScanKeysAsync(match string, fn func([]string) error) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	// Call the function with an empty array as Ristretto doesn't support scanning
	return fn([]string{})
}

// LocalScanMap is a placeholder for Redis HSCAN operation
func LocalScanMap[V any](key string, match string) (map[string]V, error) {
	if localClient == nil {
		return nil, ErrLocalDBNotInit
	}

	// Just return the full map since Ristretto doesn't support pattern matching
	return LocalGetMap[V](key)
}

// LocalScanMapAsync is a placeholder for Redis HSCAN operation with callback
func LocalScanMapAsync[V any](key string, match string, fn func(map[string]V) error) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	// Get the full map and call the function
	m, err := LocalGetMap[V](key)
	if err != nil {
		return err
	}

	return fn(m)
}

// LocalSetNX sets a key-value pair only if the key doesn't exist
func LocalSetNX[T any](key string, value T, expire time.Duration) (bool, error) {
	if localClient == nil {
		return false, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key already exists
	_, found := localClient.cache.Get(serializedKey)
	if found {
		return false, nil
	}

	// Marshal the value
	bytes, err := parser.MarshalCBOR(value)
	if err != nil {
		return false, err
	}

	// Set expiration time if provided
	if expire > 0 {
		expireAt := time.Now().Add(expire).UnixNano()
		localClient.keyExpires.Store(serializedKey, expireAt)
	}

	// Store the value
	localClient.cache.Set(serializedKey, bytes, 1)
	return true, nil
}

var (
	ErrLocalLockTimeout = errors.New("local lock timeout")
)

// LocalLock implements a simple mutex lock in the local cache
func LocalLock(key string, expire time.Duration, tryLockTimeout time.Duration) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)
	endTime := time.Now().Add(tryLockTimeout)

	for time.Now().Before(endTime) {
		success, err := LocalSetNX(serializedKey, "1", expire)
		if err != nil {
			return err
		}

		if success {
			return nil
		}

		// Wait a bit before trying again
		time.Sleep(20 * time.Millisecond)
	}

	return ErrLocalLockTimeout
}

// LocalUnlock releases a lock from the local cache
func LocalUnlock(key string) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	_, err := LocalDel(key)
	return err
}

// LocalExpire sets an expiration time for a key
func LocalExpire(key string, duration time.Duration) (bool, error) {
	if localClient == nil {
		return false, ErrLocalDBNotInit
	}

	serializedKey := localSerialKey(key)

	// Check if the key exists
	_, found := localClient.cache.Get(serializedKey)
	if !found {
		return false, nil
	}

	// Set expiration time
	expireAt := time.Now().Add(duration).UnixNano()
	localClient.keyExpires.Store(serializedKey, expireAt)
	return true, nil
}

// LocalTransaction is a placeholder for Redis transaction
// Since Ristretto doesn't support transactions, we just execute the function
func LocalTransaction(fn func() error) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	// Just execute the function as Ristretto doesn't support transactions
	return fn()
}

// LocalPublish publishes a message to a channel
func LocalPublish(channel string, message any) error {
	if localClient == nil {
		return ErrLocalDBNotInit
	}

	// Convert message to string if it's not already
	if _, ok := message.(string); !ok {
		message = parser.MarshalJson(message)
	}

	localClient.pubsubMutex.RLock()
	subscribers, exists := localClient.pubsubChannels[channel]
	localClient.pubsubMutex.RUnlock()

	if !exists {
		return nil
	}

	// Send message to all subscribers
	for ch := range subscribers {
		select {
		case ch <- message:
			// Message sent
		default:
			// Channel is full or closed, skip
		}
	}

	return nil
}

// LocalSubscribe subscribes to a channel
func LocalSubscribe[T any](channel string) (<-chan T, func()) {
	ch := make(chan T, 100) // Buffer size to prevent blocking

	if localClient == nil {
		close(ch)
		return ch, func() {}
	}

	// Register the subscriber
	localClient.pubsubMutex.Lock()
	if _, exists := localClient.pubsubChannels[channel]; !exists {
		localClient.pubsubChannels[channel] = make(map[chan interface{}]bool)
	}

	// Use a separate channel for interface{} type and convert to T
	rawCh := make(chan interface{}, 100)
	localClient.pubsubChannels[channel][rawCh] = true
	localClient.pubsubMutex.Unlock()

	// Start a goroutine to convert messages from rawCh to ch
	go func() {
		defer close(ch)
		for msg := range rawCh {
			if strMsg, ok := msg.(string); ok {
				value, err := parser.UnmarshalJson[T](strMsg)
				if err == nil {
					ch <- value
				}
			}
		}
	}()

	// Return unsubscribe function
	return ch, func() {
		localClient.pubsubMutex.Lock()
		if subscribers, exists := localClient.pubsubChannels[channel]; exists {
			delete(subscribers, rawCh)
			if len(subscribers) == 0 {
				delete(localClient.pubsubChannels, channel)
			}
		}
		localClient.pubsubMutex.Unlock()
		close(rawCh)
	}
}
