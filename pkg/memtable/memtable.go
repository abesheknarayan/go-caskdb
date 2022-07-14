package memtable

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	CustomError "github.com/abesheknarayan/go-caskdb/pkg/error"
	"github.com/abesheknarayan/go-caskdb/pkg/format"
	KeyEntry "github.com/abesheknarayan/go-caskdb/pkg/key_entry"
	"github.com/abesheknarayan/go-caskdb/pkg/utils"
	"github.com/sirupsen/logrus"
)

/*
	- total size of key + value as this shouldnt cross 4KB
	- segment number
	- put / get / delete methods
	-

*/

type HashMap struct {
	M  map[string]KeyEntry.KeyEntry
	Mu *sync.Mutex
}

type MemTable struct {
	DbName        string
	BytesOccupied uint64 // total nunber of bytes occupied
	Map           *HashMap
	SegmentId     int32
	Mu            *sync.Mutex
	Wg            *sync.WaitGroup
}

func GetNewMemTable(dbName string, SegmentId int32) *MemTable {

	memtable := &MemTable{
		DbName:        dbName,
		BytesOccupied: 0,
		Map:           &HashMap{M: make(map[string]KeyEntry.KeyEntry), Mu: &sync.Mutex{}},
		Mu:            &sync.Mutex{},
		Wg:            &sync.WaitGroup{},
		SegmentId:     int32(SegmentId),
	}

	return memtable
}

func (mt *MemTable) Get(key string) (string, error) {
	mt.Map.Mu.Lock()
	defer mt.Map.Mu.Unlock()
	kv, exist := mt.Map.M[key]

	if !exist {
		return "", CustomError.ErrKeyDoesNotExist
	}

	return kv.Value, nil
}

func (mt *MemTable) Put(key string, value string) error {
	// var l = utils.Logger.WithFields(logrus.Fields{
	// 	"method":      "Put",
	// 	"param_key":   key,
	// 	"param_value": value,
	// })

	mt.Map.Mu.Lock()
	defer mt.Map.Mu.Unlock()

	oldKeyEntry, alreadyExists := mt.Map.M[key]

	oldBytes := 0

	if alreadyExists {
		oldBytes = len(key) + len(oldKeyEntry.Value) + 8
	}
	newBytes := len(key) + len(value) + 8

	if mt.BytesOccupied+uint64(newBytes-oldBytes) > config.Config.MemtableSizeLimit {
		// copy all the memtable to segment file --> disk write
		return CustomError.ErrMaxSizeExceeded
	}

	mt.Map.M[key] = KeyEntry.KeyEntry{
		Timestamp: time.Now().Unix(),
		Value:     value,
	}
	// l.Debugf("Stored value to map at %d", mt.Map.M[key].Timestamp)
	mt.BytesOccupied += uint64(newBytes - oldBytes) // 8 for timestamp

	return nil
}

func (mt *MemTable) LoadFromSegmentFile(SegmentId uint32) error {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "LoadFromSegmentFile",
	})
	l.Infof("Attempting to load segment file with id %d of db %s", SegmentId, mt.DbName)

	path := config.Config.Path

	segmentFilePath := fmt.Sprintf("%s/%s/%d.seg", path, mt.DbName, SegmentId)

	f, err := os.Open(segmentFilePath)

	if err != nil {
		l.Errorf("Error while opening segment file for db %s: %v", mt.DbName, err)
	}

	reader := bufio.NewReader(f)

	for {
		header := make([]byte, format.HEADER_SIZE)
		_, err := io.ReadFull(reader, header) // to read exactly HEADER_SIZE bytes
		if err != nil {
			break
		}
		timestamp, key_size, value_size := format.DecodeHeader(header)
		keyBuf := make([]byte, key_size)
		valueBuf := make([]byte, value_size)

		_, err = io.ReadFull(reader, keyBuf)

		if err != nil {
			return err
		}

		_, err = io.ReadFull(reader, valueBuf)

		if err != nil {
			return err
		}
		// update bytesOccupied of memtable
		mt.BytesOccupied += uint64(format.HEADER_SIZE + 8 + key_size + value_size)

		key := string(keyBuf)
		value := string(valueBuf)
		kv := KeyEntry.KeyEntry{
			Timestamp: timestamp,
			Value:     value,
		}
		mt.Map.M[key] = kv
	}
	mt.SegmentId = int32(SegmentId)
	return nil
}

// returns the (written segment file name, whether it already existed, cardinality of segment) along with error
func (mt *MemTable) WriteMemtableToDisk() (uint32, bool, error) {

	mt.Map.Mu.Lock()
	defer mt.Map.Mu.Unlock()

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "WriteMemtableToDisk",
	})
	l.Infof("Writing Memtable %d to Segment file !!", mt.SegmentId)

	path := config.Config.Path

	segmentFileName := fmt.Sprintf("%d.seg", mt.SegmentId)

	segmentFilePath := fmt.Sprintf("%s/%s/%s", path, mt.DbName, segmentFileName)

	var exists bool = true

	if _, err := os.Stat(segmentFilePath); errors.Is(err, os.ErrNotExist) {
		// file doesnt exist
		exists = false
	}
	l.Debugln(exists)

	f, err := os.OpenFile(segmentFilePath, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		l.Errorf("Error in opening segment file %s : %v", segmentFilePath, err)
		return 0, false, CustomError.ErrOpeningSegmentFile
	}

	// truncate the file
	f.Truncate(0)

	// Golang map doesnt print the elements in the order of sorted keys
	// Get all keys, sort it yourself and then retrieve the corresponding values from map

	sortedKeys := []string{}

	for key := range mt.Map.M {
		sortedKeys = append(sortedKeys, key)
	}

	sort.Strings(sortedKeys)

	// write the map contents as bytes
	var bytesArr []byte

	for _, key := range sortedKeys {
		kv := mt.Map.M[key]
		_, data := format.EncodeKeyValue(kv.Timestamp, key, kv.Value)
		bytesArr = append(bytesArr, data...)
	}

	f.Write(bytesArr)
	f.Sync() // to flush from OS buffer to disk
	f.Close()

	l.Debugf("Successfully written memtable to segfile %s with cardinality: %d", segmentFileName, uint32(len(sortedKeys)))

	return uint32(len(sortedKeys)), exists, nil
}

func (mt *MemTable) Contains(key string) bool {
	mt.Map.Mu.Lock()
	defer mt.Map.Mu.Unlock()
	_, ok := mt.Map.M[key]
	return ok
}

// Clears the memtable
func (mt *MemTable) Clear() {
	for k := range mt.Map.M {
		delete(mt.Map.M, k)
	}
	mt.BytesOccupied = 0
}
