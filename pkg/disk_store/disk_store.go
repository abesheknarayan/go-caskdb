package disk_store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	CustomError "github.com/abesheknarayan/go-caskdb/pkg/error"
	KeyEntry "github.com/abesheknarayan/go-caskdb/pkg/key_entry"
	"github.com/abesheknarayan/go-caskdb/pkg/memtable"
	"github.com/abesheknarayan/go-caskdb/pkg/utils"
	"github.com/sirupsen/logrus"
)

// // for now, support keys and values only with string type

// contains the metadata of segment files which goes in the manifest file
type SegmentMetadata struct {
	SegmentId   uint32
	Cardinality uint32 // no of keys it contains
}

// contains the metdata of DB
type Manifest struct {
	DbName   string
	Segments []SegmentMetadata // should always be sorted according to SegmentId
}

type HashIndex map[string]KeyEntry.KeyEntry

type DiskStore struct {
	Manifest          *Manifest
	ManifestFile      *os.File  // holding the file to prevent unnecessary opening and closing everytime [subject to change in future]
	HashIndex         HashIndex // map of any value type
	Memtable          *memtable.MemTable
	AuxillaryMemtable *memtable.MemTable // memtable is copied to this while its being written asynchronously to disk
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
	manifestFile := fmt.Sprintf("%s/manifest.json", dirPath)

	if _, err := os.Stat(manifestFile); errors.Is(err, os.ErrNotExist) {
		l.Infoln("file doesn't exist !!")
		return createDb(dbName, dirPath)
	}

	// open manifest file in rw mode
	f, err := os.OpenFile(manifestFile, os.O_RDWR, 0666)

	if err != nil {
		// prolly os error
		return nil, err
	}

	manifest := LoadManifest(f)

	d := &DiskStore{
		Manifest:          manifest,
		ManifestFile:      f,
		HashIndex:         HashIndex{},
		Memtable:          memtable.GetNewMemTable(dbName),
		AuxillaryMemtable: nil,
	}

	// load the most recent segment file onto memtable
	d.Memtable.LoadFromSegmentFile(d.Manifest.Segments[len(d.Manifest.Segments)-1].SegmentId)

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

// create new db
func createDb(dbName string, dbPath string) (*DiskStore, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":       "createDb",
		"param_dbName": dbName,
		"param_path":   dbPath,
	})
	l.Infoln("Attempting to create a database")

	// first create manifest file
	filename := fmt.Sprintf("%s/manifest.json", dbPath)
	l.Infof("creating new file %s\n", filename)
	manifestFile, err := os.Create(filename)

	if err != nil {
		return nil, err
	}

	manifest := &Manifest{
		DbName:   dbName,
		Segments: []SegmentMetadata{},
	}

	if err != nil {
		return nil, err
	}

	encoder := json.NewEncoder(manifestFile)
	encoder.Encode(manifest)

	d := &DiskStore{
		Manifest:          manifest,
		ManifestFile:      manifestFile,
		HashIndex:         HashIndex{},
		Memtable:          memtable.GetNewMemTable(dbName),
		AuxillaryMemtable: nil,
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
		"method":      "Put",
		"param_key":   key,
		"param_value": value,
	})
	l.Infof("Attempting to set a key")

	if err := d.Memtable.Put(key, value); errors.Is(err, CustomError.MaxSizeExceedError) {
		// copy memtable to aux memtable
		// since it's a pointer just change the pointers

		// if its not nil then before auxillary memtable is waiting to write its contents to file and got blocked because of file write. So we block the main go routine so that, the auxillary file write finishes before executing further
		// Important point to note here is that, during the time between auxillary go routine waiting to write to this step in the next run, all writes and reads are supported using memtable and aux memtable so no issues with reads and writes
		if d.AuxillaryMemtable != nil {
			l.Debugln("Waiting for aux memtable write to disk to finish")
			d.AuxillaryMemtable.Wg.Wait()
		}
		l.Debugln("Writing memtable to aux")
		d.AuxillaryMemtable = d.Memtable
		d.Memtable = memtable.GetNewMemTable(d.Manifest.DbName)

		// async
		go func() {
			// find segment id
			l.Debugln("Writing Auxillary memtable to disk")
			l.Debugln(d.AuxillaryMemtable)
			d.AuxillaryMemtable.Wg.Add(1)
			segmentId := d.GetNewSegmentId()
			cardinality, err := d.AuxillaryMemtable.WriteMemtableToDisk(segmentId)
			if err != nil {
				l.Fatalln(err)
			}
			d.Manifest.Segments = append(d.Manifest.Segments, SegmentMetadata{
				SegmentId:   segmentId,
				Cardinality: cardinality,
			})
			d.ChangeNumberOfSegmentsInManifest()
			d.AuxillaryMemtable.Wg.Done()
		}()

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
		// check auxillary memtable

		if d.AuxillaryMemtable != nil {

			value, err = d.AuxillaryMemtable.Get(key)

			if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
				// check all the segments one by one from the most recent

				value, err = d.CheckAllSegmentsOneByOne(key, int(len(d.Manifest.Segments)-1))

				if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
					return ""
				}
			}
			return value
		}
	}

	return value
}

// Returns the most recent segment id plus 1 from the disk store
func (d *DiskStore) GetNewSegmentId() uint32 {
	if len(d.Manifest.Segments) == 0 {
		return 1
	}
	return d.Manifest.Segments[len(d.Manifest.Segments)-1].SegmentId + 1
}

func (d *DiskStore) CheckAllSegmentsOneByOne(key string, segmentIndex int) (string, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":              "CheckAllSegmentsOneByOne",
		"param_key":           key,
		"param_segmentNumber": segmentIndex,
	})
	if segmentIndex < 0 {
		return "", CustomError.KeyDoesNotExistError
	}
	l.Infof("Attempting to check segment file %d for key %s", segmentIndex, key)

	memtable := memtable.GetNewMemTable(d.Manifest.DbName)
	memtable.LoadFromSegmentFile(d.Manifest.Segments[segmentIndex].SegmentId)

	value, err := memtable.Get(key)
	if err != nil && errors.Is(err, CustomError.KeyDoesNotExistError) {
		// check before segment file recursively
		return d.CheckAllSegmentsOneByOne(key, segmentIndex-1)
	}

	return value, nil
}

// clears the db
func (d *DiskStore) Cleanup() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "Cleanup",
	})
	l.Infoln("Cleaning up the database")

	// clear the segments slice
	d.Manifest.Segments = d.Manifest.Segments[:0]

	// delete everything including manifest file

	path := config.Config.Path
	dirPath := fmt.Sprintf("%s/%s", path, d.Manifest.DbName)
	err := os.RemoveAll(dirPath)
	if err != nil {
		l.Errorln(err)
	}
}

func (d *DiskStore) ChangeNumberOfSegmentsInManifest() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "ChangeNumberOfSegmentsInManifest",
	})
	err := d.ManifestFile.Truncate(0)

	if err != nil {
		l.Panicf("Error in truncating manifest file %v", err)
	}

	marshalledManifest, err := json.Marshal(d.Manifest)
	if err != nil {
		l.Panicf("Error in marshalling  manifest obejct %v", err)
	}

	manifestFile := fmt.Sprintf("%s/%s/manifest.json", config.Config.Path, d.Manifest.DbName)
	// err = encoder.Encode(manifest)
	err = ioutil.WriteFile(manifestFile, marshalledManifest, 0666)

	if err != nil {
		l.Panicf("Error in writing to manifest file %v", err)
	}
}

// Deletes the contents of memtable
func (d *DiskStore) CloseDB() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "CloseDB",
	})
	l.Infoln("Closing the database")

	// write memtable to segment file and clear it
	segmentId := d.GetNewSegmentId()
	cardinality, err := d.Memtable.WriteMemtableToDisk(segmentId)
	if err != nil {
		l.Fatalf("Error while writing memtable to disk %v", err)
	}
	d.Manifest.Segments = append(d.Manifest.Segments, SegmentMetadata{
		SegmentId:   segmentId,
		Cardinality: cardinality,
	})
	d.ChangeNumberOfSegmentsInManifest()
	d.Memtable.Clear()

	// close manifest file
	d.ManifestFile.Close()
}
