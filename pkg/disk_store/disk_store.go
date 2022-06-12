package disk_store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	CustomError "github.com/abesheknarayan/go-caskdb/pkg/error"
	KeyEntry "github.com/abesheknarayan/go-caskdb/pkg/key_entry"
	"github.com/abesheknarayan/go-caskdb/pkg/memtable"
	"github.com/abesheknarayan/go-caskdb/pkg/utils"
	"github.com/sirupsen/logrus"
)

// // for now, support keys and values only with string type

// contains the metdata of DB
type Manifest struct {
	DbName           string
	NumberOfSegments uint32 // number of segments created till now
}

type HashIndex map[string]KeyEntry.KeyEntry

type DiskStore struct {
	Manifest  *Manifest
	HashIndex HashIndex // map of any value type
	Memtable  *memtable.MemTable
}

// creates a new db and returns the object ref
func InitDb(dbName string) (*DiskStore, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "InitDb",
	})

	path := config.Config.Path

	dirPath := fmt.Sprintf("%s/%s", path, dbName)

	// check if dir $db_name/ exists
	if _, err := os.Stat(dirPath); errors.Is(err, os.ErrNotExist) {
		// directory doesn't exist
		l.Infoln("Directory doesn't exist, creating new one !!")
		err = os.Mkdir(dirPath, 0777)

		if err != nil {
			l.Errorf("Error while creating new directory for db %v", err)
			return nil, err
		}
	}

	// if db manifest file is already present load it or else create new db
	manifestFile := fmt.Sprintf("%s/%s.json", dirPath, dbName)

	if _, err := os.Stat(manifestFile); errors.Is(err, os.ErrNotExist) {
		l.Infoln("file doesn't exist !!")
		return createDb(dbName, dirPath)
	}

	// open manifest file in rw mode
	f, err := os.OpenFile(manifestFile, os.O_RDONLY, 0666)
	defer func() {
		err = f.Close()
		if err != nil {
			l.Fatalln(err)
		}
	}()

	if err != nil {
		// prolly os error
		return nil, err
	}

	manifest := LoadManifest(f)

	var segmentNumber uint32
	if manifest.NumberOfSegments > 0 {
		segmentNumber = manifest.NumberOfSegments
	} else {
		segmentNumber = manifest.NumberOfSegments + 1
	}

	d := &DiskStore{
		Manifest:  manifest,
		HashIndex: HashIndex{},
		Memtable:  memtable.GetNewMemTable(dbName, segmentNumber),
	}

	d.Memtable.LoadFromSegmentFile()

	// TODO
	// d.loadHashIndex()

	return d, nil
}

func LoadManifest(f *os.File) *Manifest {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "LoadManifest",
	})

	manifest := &Manifest{}
	content, err := io.ReadAll(f)

	if err != nil {
		l.Errorln(err)
	}

	json.Unmarshal(content, &manifest)

	return manifest
}

// create new file
func createDb(dbName string, dbPath string) (*DiskStore, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":       "createDb",
		"param_dbName": dbName,
		"param_path":   dbPath,
	})
	l.Infoln("Attempting to create a database")

	// first create manifest file
	filename := fmt.Sprintf("%s/%s.json", dbPath, dbName)
	l.Infof("creating new file %s\n", filename)
	manifestFile, err := os.Create(filename)

	if err != nil {
		return nil, err
	}

	manifest := &Manifest{
		DbName:           dbName,
		NumberOfSegments: 0,
	}

	if err != nil {
		return nil, err
	}

	encoder := json.NewEncoder(manifestFile)
	encoder.Encode(manifest)

	d := &DiskStore{
		Manifest:  manifest,
		HashIndex: HashIndex{},
		Memtable:  memtable.GetNewMemTable(dbName, manifest.NumberOfSegments+1),
	}

	// TODO
	// err = d.loadHashIndex()
	// if err != nil {
	// 	return nil, err
	// }

	return d, nil
}

func (d *DiskStore) Put(key string, value string) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":      "Set",
		"param_key":   key,
		"param_value": value,
	})
	l.Infof("Attempting to set a key")

	if err := d.Memtable.Put(key, value); errors.Is(err, CustomError.MaxSizeExceedError) {
		err = d.Memtable.WriteMemtableToDisk()

		if err != nil {
			l.Fatalln(err)
		}

		d.Manifest.NumberOfSegments += 1
		d.ChangeManifestFileContent(d.Manifest.NumberOfSegments)

		d.Memtable = memtable.GetNewMemTable(d.Manifest.DbName, d.Manifest.NumberOfSegments+1)

		// again call
		d.Memtable.Put(key, value)
	}
}

func (d *DiskStore) Get(key string) string {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method":    "Get",
		"param_key": key,
	})
	l.Infoln("Attempting to get value for key")
	value, err := d.Memtable.Get(key)

	if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
		// check all the segments one by one from the most recent
		value, err = d.CheckAllSegmentsOneByOne(key, d.Manifest.NumberOfSegments)

		if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
			return ""
		}
	}

	return value
}

func (d *DiskStore) CheckAllSegmentsOneByOne(key string, segmentNumber uint32) (string, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":              "CheckAllSegmentsOneByOne",
		"param_key":           key,
		"param_segmentNumber": segmentNumber,
	})
	if segmentNumber == 0 {
		return "", CustomError.KeyDoesNotExistError
	}
	l.Infof("Attempting to check segment file %d for key %s", segmentNumber, key)

	memtable := memtable.GetNewMemTable(d.Manifest.DbName, segmentNumber)
	memtable.LoadFromSegmentFile()

	value, err := memtable.Get(key)
	if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
		// check before segment file recursively
		return d.CheckAllSegmentsOneByOne(key, segmentNumber-1)
	}

	return value, nil
}

// clears the db
func (d *DiskStore) Cleanup() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "Cleanup",
	})
	l.Infoln("Cleaning up the database")
	d.Manifest.NumberOfSegments = 0

	// delete everything including manifest file

	path := config.Config.Path
	dirPath := fmt.Sprintf("%s/%s", path, d.Manifest.DbName)
	err := os.RemoveAll(dirPath)
	if err != nil {
		l.Errorln(err)
	}
}

func (d *DiskStore) ChangeManifestFileContent(numberOfSegments uint32) {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method":                 "ChangeManifestFileContent",
		"param_numberOfSegments": numberOfSegments,
	})
	path := config.Config.Path
	// modify manifest
	manifestFilePath := fmt.Sprintf("%s/%s/%s.json", path, d.Manifest.DbName, d.Manifest.DbName)

	manifestFile, err := os.OpenFile(manifestFilePath, os.O_RDWR, 0666)

	if err != nil {
		l.Errorf("Error in opening manifest file %v", err)
	}

	manifest := &Manifest{
		DbName:           d.Manifest.DbName,
		NumberOfSegments: numberOfSegments,
	}
	manifestFile.Truncate(0)

	encoder := json.NewEncoder(manifestFile)
	encoder.Encode(manifest)
}

// Deletes the contents of memtable
func (d *DiskStore) CloseDB() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "CloseDB",
	})
	l.Infoln("Closing the database")

	// write memtable to segment file and clear it
	d.Memtable.WriteMemtableToDisk()
	d.Memtable.Clear()
}
