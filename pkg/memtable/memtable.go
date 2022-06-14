package memtable

import (
	"bufio"
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

type HashMap map[string]KeyEntry.KeyEntry

const MAXSIZE uint64 = 4 * 1024

type MemTable struct {
	DbName        string
	BytesOccupied uint64 // total nunber of bytes occupied
	Memtable      HashMap
	Wg            *sync.WaitGroup
}

func GetNewMemTable(dbName string) *MemTable {

	memtable := &MemTable{
		DbName:        dbName,
		BytesOccupied: 0,
		Memtable:      make(HashMap),
		Wg:            &sync.WaitGroup{},
	}

	return memtable
}

func (mt *MemTable) Get(key string) (string, error) {
	kv, exist := mt.Memtable[key]

	if !exist {
		return "", CustomError.KeyDoesNotExistError
	}

	return kv.Value, nil
}

func (mt *MemTable) Put(key string, value string) error {

	mt.Memtable[key] = KeyEntry.KeyEntry{
		Timestamp: time.Now().Unix(),
		Value:     value,
	}

	if mt.BytesOccupied+uint64(len(key)+len(value)) > MAXSIZE {
		// copy all the memtable to segment file --> disk write
		return CustomError.MaxSizeExceedError
	}

	mt.BytesOccupied += uint64((len(key) + len(value) + 8)) // 8 for timestamp

	return nil
}

func (mt *MemTable) LoadFromSegmentFile(segmentId uint32) error {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "LoadFromSegmentFile",
	})
	l.Infof("Attempting to load segment file with id %d of db %s", segmentId, mt.DbName)

	path := config.Config.Path

	segmentFilePath := fmt.Sprintf("%s/%s/%d.seg", path, mt.DbName, segmentId)

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
		mt.Memtable[key] = kv
	}
	return nil
}

// returns the (written segment file name, timestamp, cardinality of segment) along with error
func (mt *MemTable) WriteMemtableToDisk(segmentId uint32) (uint32, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "WriteMemtableToDisk",
	})
	l.Info("Writing Memtable to Segment file !!")

	path := config.Config.Path

	segmentFileName := fmt.Sprintf("%d.seg", segmentId)

	segmentFilePath := fmt.Sprintf("%s/%s/%s", path, mt.DbName, segmentFileName)

	f, err := os.OpenFile(segmentFilePath, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		l.Errorf("Error in opening segment file %s : %v", segmentFilePath, err)
		return 0, nil
	}

	// truncate the file
	f.Truncate(0)

	// Golang map doesnt print the elements in the order of sorted keys
	// Get all keys, sort it yourself and then retrieve the corresponding values from map

	sortedKeys := []string{}

	for key := range mt.Memtable {
		sortedKeys = append(sortedKeys, key)
	}

	sort.Strings(sortedKeys)

	// write the map contents as bytes
	var bytesArr []byte

	for _, key := range sortedKeys {
		kv := mt.Memtable[key]
		_, data := format.EncodeKeyValue(kv.Timestamp, key, kv.Value)
		bytesArr = append(bytesArr, data...)
	}

	f.Write(bytesArr)
	f.Sync() // to flush from OS buffer to disk
	f.Close()

	l.Debugf("Successfully written memtable to segfile %s with cardinality: %d", segmentFileName, uint32(len(sortedKeys)))

	return uint32(len(sortedKeys)), nil
}

// Clears the memtable
func (mt *MemTable) Clear() {
	for k := range mt.Memtable {
		delete(mt.Memtable, k)
	}
	mt.BytesOccupied = 0
}
