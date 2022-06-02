package stores

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/abesheknarayan/go-caskdb/format"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// for now, support keys and values only with string type

type KeyEntry struct {
	timestamp int64
	position  int32
	size      int32
}

type HashIndex map[string]KeyEntry

type DiskStore struct {
	currentByteOffsetPosition int32
	file                      *os.File
	filename                  string
	hashIndex                 HashIndex // map of any value type
}

// creates a new db and returns the object ref
func InitDb(dbName string, path string) (*DiskStore, error) {

	var l = log.WithFields(logrus.Fields{
		"method": "InitDb",
	})

	// if db is already present load it or else create new db
	fileName := fmt.Sprintf("%s/%s.db", path, dbName)

	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		l.Infoln("file doesn't exist !!")
		return createDb(dbName, path)
	}

	// open file in binary + append mode
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND, 0666)

	if err != nil {
		// prolly os error
		return nil, err
	}

	d := &DiskStore{
		filename:                  fileName,
		file:                      f,
		currentByteOffsetPosition: 0,
		hashIndex:                 HashIndex{},
	}
	d.loadHashIndex()
	return d, nil
}

// create new file
func createDb(dbName string, path string) (*DiskStore, error) {

	var l = log.WithFields(logrus.Fields{
		"method":       "createDb",
		"param_dbName": dbName,
		"param_path":   path,
	})
	l.Infoln("Attempting to create a database")

	filename := fmt.Sprintf("%s/%s.db", path, dbName)
	l.Infof("creating new file %s\n", filename)
	f, err := os.Create(filename)

	if err != nil {
		return nil, err
	}

	d := &DiskStore{
		filename:                  filename,
		file:                      f,
		currentByteOffsetPosition: 0,
		hashIndex:                 HashIndex{},
	}
	err = d.loadHashIndex()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DiskStore) loadHashIndex() error {
	// load hash map from file
	// read byte by byte
	reader := bufio.NewReader(d.file)
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

		key := string(keyBuf)
		sz := format.HEADER_SIZE + key_size + value_size
		kv := KeyEntry{
			timestamp: timestamp,
			position:  d.currentByteOffsetPosition,
			size:      sz,
		}
		d.hashIndex[key] = kv
		d.currentByteOffsetPosition += sz
	}
	return nil
}

func (d *DiskStore) Set(key string, value string) {

	var l = log.WithFields(logrus.Fields{
		"method":      "Set",
		"param_key":   key,
		"param_value": value,
	})
	l.Infof("Attempting to set a key")

	// store to disk
	timestamp := time.Now().Unix()
	sz, data := format.EncodeKeyValue(timestamp, key, value)
	d.writeWithSync(data)
	kv := KeyEntry{
		timestamp: timestamp,
		position:  d.currentByteOffsetPosition,
		size:      sz,
	}
	d.hashIndex[key] = kv
	d.currentByteOffsetPosition += sz
}

func (d *DiskStore) writeWithSync(data []byte) {
	d.file.Write(data)
	d.file.Sync() // fsync - bypasses os cache and directly stores into the file for reliability
}

func (d *DiskStore) Get(key string) string {
	// get key from db
	var l = log.WithFields(logrus.Fields{
		"method":    "Get",
		"param_key": key,
	})
	l.Infoln("Attempting to get value for key")

	kv, ok := d.hashIndex[key]
	if !ok {
		return ""
	}
	d.file.Seek(int64(kv.position), format.DEFAULT_WHENCE)
	dataByte := make([]byte, kv.size)
	n, err := d.file.Read(dataByte)

	if err != nil || int32(n) != kv.size {
		l.Errorln(err)
	}

	_, _, value := format.DecodeKeyValue(dataByte)
	return value
}

// clears the db file and hash index
func (d *DiskStore) Cleanup() {
	var l = log.WithFields(logrus.Fields{
		"method": "Cleanup",
	})
	l.Infoln("Cleaning up the database")

	d.currentByteOffsetPosition = 0
	for k := range d.hashIndex {
		delete(d.hashIndex, k)
	}
	os.Remove(d.filename)
}

func (d *DiskStore) CloseDB() {
	var l = log.WithFields(logrus.Fields{
		"method": "CloseDB",
	})
	l.Infoln("Closing the database")

	d.file.Sync()
	d.file.Close()
}
