package disk_store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

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
	Cardinality uint32      // no of keys it contains
	Mu          *sync.Mutex `json:"-"`
}

type SegmentLevelMetadata struct {
	Segments []SegmentMetadata
	Mu       *sync.Mutex `json:"-"`
}

// contains the metdata of DB
type Manifest struct {
	DbName         string
	NumberOfLevels uint32                 // levels start from 0 to NumberOfLevels - 1
	SegmentLevels  []SegmentLevelMetadata // should always be sorted according to SegmentId
	MaxSegmentId   uint32                 // maximum segmend id of all segments to get newer segment ids easily
	Mu             *sync.Mutex            `json:"-"` // omit the field for json
}

type HashIndex map[string]KeyEntry.KeyEntry

type DiskStore struct {
	Manifest          *Manifest
	ManifestFile      *os.File  // holding the file to prevent unnecessary opening and closing everytime [subject to change in future]
	HashIndex         HashIndex // map of any value type
	Memtable          *memtable.MemTable
	AuxillaryMemtable *memtable.MemTable // memtable is copied to this while its being written asynchronously to disk
	MergeCompactor    []MergeCompactor
	MergeCompactorWg  *sync.WaitGroup
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
		Memtable:          memtable.GetNewMemTable(dbName, int32(manifest.MaxSegmentId)),
		AuxillaryMemtable: nil,
		MergeCompactor:    []MergeCompactor{},
		MergeCompactorWg:  &sync.WaitGroup{},
	}

	// initiate sync.Mutex locks for segement leveels and segments and merge comparator for each level
	for i := 0; i < int(d.Manifest.NumberOfLevels); i++ {

		d.MergeCompactor = append(d.MergeCompactor, MergeCompactor{Mu: &sync.Mutex{}})
		d.Manifest.SegmentLevels[i].Mu = &sync.Mutex{}

		for j := 0; j < len(d.Manifest.SegmentLevels[i].Segments); j++ {
			d.Manifest.SegmentLevels[i].Segments[j].Mu = &sync.Mutex{}
		}
	}

	// load the level 0 segment file if it exists
	if d.Manifest.NumberOfLevels > 0 {
		d.Memtable.LoadFromSegmentFile(d.Manifest.SegmentLevels[0].Segments[len(d.Manifest.SegmentLevels[0].Segments)-1].SegmentId)
	}

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

	manifest.Mu = &sync.Mutex{}

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
		DbName:         dbName,
		NumberOfLevels: 0,
		MaxSegmentId:   1, // 1 because initial memtable will be creating with segment id = 1, if 0 is needed then change it in both places
		SegmentLevels:  []SegmentLevelMetadata{},
		Mu:             &sync.Mutex{},
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
		Memtable:          memtable.GetNewMemTable(dbName, 1),
		AuxillaryMemtable: nil,
		MergeCompactor:    []MergeCompactor{},
		MergeCompactorWg:  &sync.WaitGroup{},
	}

	return d, nil
}

func (d *DiskStore) Put(key string, value string) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":      "Put",
		"param_key":   key,
		"param_value": value,
	})
	l.Infof("Attempting to set a key")

	if err := d.Memtable.Put(key, value); errors.Is(err, CustomError.ErrMaxSizeExceeded) {
		// copy memtable to aux memtable
		// since it's a pointer just change the pointers

		// if its not nil then before auxillary memtable is waiting to write its contents to file and got blocked because of file write. So we block the main go routine so that, the auxillary file write finishes before executing further
		// Important point to note here is that, during the time between auxillary go routine waiting to write to this step in the next run, all writes and reads are supported using memtable and aux memtable so no issues with reads and writes
		if d.AuxillaryMemtable != nil {
			l.Infoln("Waiting for aux memtable write to disk to finish")
			// d.AuxillaryMemtable.Mu.Lock()
			d.AuxillaryMemtable.ExWaitGroup.Mu.Lock()
			d.AuxillaryMemtable.ExWaitGroup.Wg.Wait()
			d.AuxillaryMemtable.ExWaitGroup.Mu.Unlock()
			// d.AuxillaryMemtable.Mu.Unlock()
		}
		l.Infoln("Writing memtable to aux")
		if d.AuxillaryMemtable == nil {
			d.AuxillaryMemtable = memtable.GetNewMemTable(d.Manifest.DbName, d.Memtable.SegmentId)
		}
		d.AuxillaryMemtable.Mu.Lock()
		d.AuxillaryMemtable.CopyMemtable(d.Memtable)
		d.AuxillaryMemtable.Mu.Unlock()
		d.Memtable = memtable.GetNewMemTable(d.Manifest.DbName, int32(d.GetNewSegmentId()))

		go func() {
			// find segment id
			l.Infoln("Writing Auxillary memtable to disk")

			d.AuxillaryMemtable.ExWaitGroup.Mu.Lock()
			d.AuxillaryMemtable.ExWaitGroup.Wg.Add(1)

			d.Manifest.Mu.Lock()
			if d.Manifest.NumberOfLevels == 0 {
				d.Manifest.NumberOfLevels = 1
				d.Manifest.SegmentLevels = append(d.Manifest.SegmentLevels, SegmentLevelMetadata{
					Segments: []SegmentMetadata{},
					Mu:       &sync.Mutex{},
				})
				d.InitMergeCompactor(0)
			}
			d.Manifest.Mu.Unlock()
			l.Debugln("here 2")

			// send a compaction signal to level 0 channel which will be listening somewhere
			cardinality, exists, err := d.AuxillaryMemtable.WriteMemtableToDisk() // this is the writing to disk function
			if err != nil {
				l.Fatalln(err)
			}
			l.Debugln("here 3")

			// append only if its newly added file
			if !exists {
				l.Debugln("here 4")
				d.Manifest.Mu.Lock()
				d.Manifest.SegmentLevels[0].Mu.Lock()
				d.Manifest.SegmentLevels[0].Segments = append(d.Manifest.SegmentLevels[0].Segments, SegmentMetadata{
					SegmentId:   uint32(d.AuxillaryMemtable.SegmentId),
					Cardinality: cardinality,
					Mu:          &sync.Mutex{},
				})
				d.Manifest.SegmentLevels[0].Mu.Unlock()

				// unlock specifically here because next function locks it
				d.Manifest.Mu.Unlock()

				// perform merge compaction manually here
				d.WatchLevelForSizeLimitExceed(0)
			} else {
				// just update cardinality but we have to find the segment cuz it might not be in level 0
				d.FindForSegmendAndUpdate(uint32(d.AuxillaryMemtable.SegmentId), cardinality)
			}
			l.Debugln("here 5")
			d.ChangeNumberOfSegmentsInManifest()
			d.AuxillaryMemtable.ExWaitGroup.Wg.Done()
			d.AuxillaryMemtable.ExWaitGroup.Mu.Unlock()

		}()

		// again call Put
		d.Memtable.Put(key, value)
	}
}

// finds the segment object using the segment id and update its cardinality
func (d *DiskStore) FindForSegmendAndUpdate(segmentId uint32, cardinality uint32) {
	d.Manifest.Mu.Lock()
	for i := 0; i < int(d.Manifest.NumberOfLevels); i++ {
		for j := 0; j < len(d.Manifest.SegmentLevels[i].Segments); j++ {
			d.Manifest.SegmentLevels[i].Mu.Lock()
			if d.Manifest.SegmentLevels[i].Segments[j].SegmentId == segmentId {
				d.Manifest.SegmentLevels[i].Segments[j].Mu.Lock()
				d.Manifest.SegmentLevels[i].Segments[j].Cardinality = cardinality
				d.Manifest.SegmentLevels[i].Segments[j].Mu.Unlock()
			}
			d.Manifest.SegmentLevels[i].Mu.Unlock()
		}
	}
	d.Manifest.Mu.Unlock()

}

func (d *DiskStore) Get(key string) string {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method":    "Get",
		"param_key": key,
	})
	l.Infoln("Attempting to get value for key")
	value, err := d.Memtable.Get(key)

	if err == nil {
		l.Debugf("got value: %s for key %s from memtable", value, key)
		return value
	}

	if err != nil && errors.Is(err, CustomError.ErrKeyDoesNotExist) {
		// check auxillary memtable

		if d.AuxillaryMemtable != nil {
			value, err = d.AuxillaryMemtable.Get(key)
		}

		if err == nil {
			l.Debugf("got value: %s for key %s from Auxillary table", value, key)
			return value
		}

		if err != nil && errors.Is(err, CustomError.ErrKeyDoesNotExist) {
			// check all the segments one by one from the most recent

			value, err = d.ReadLevelByLevel(key)

			if err != nil && errors.Is(err, CustomError.ErrKeyDoesNotExist) {
				return ""
			}
			return value
		}
	}
	return value
}

// Reads the Segment files level by level starting from L0 to LN (where N is a variable)
func (d *DiskStore) ReadLevelByLevel(key string) (string, error) {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method":    "ReadLevelByLevel",
		"param_key": key,
	})
	l.Infof("Reading level by level for key: %s\n", key)
	d.Manifest.Mu.Lock()
	defer d.Manifest.Mu.Unlock()
	for i := uint32(0); i < uint32(d.Manifest.NumberOfLevels); i++ {
		d.Manifest.SegmentLevels[i].Mu.Lock()
		numberOfSegmentsInCurrentLevel := len(d.Manifest.SegmentLevels[i].Segments)
		d.Manifest.SegmentLevels[i].Mu.Unlock()

		val, err := d.CheckALevelForAKey(key, i, numberOfSegmentsInCurrentLevel-1)
		if err != nil && errors.Is(err, CustomError.ErrKeyDoesNotExist) {
			continue
		}
		return val, nil
	}
	return "", CustomError.ErrKeyDoesNotExist
}

// checks the segments of a level from most recent to least recent
func (d *DiskStore) CheckALevelForAKey(key string, level uint32, segmentIndex int) (string, error) {

	var l = utils.Logger.WithFields(logrus.Fields{
		"method":              "CheckALevelForAKey",
		"param_level":         level,
		"param_key":           key,
		"param_segmentNumber": segmentIndex,
	})
	if segmentIndex < 0 {
		return "", CustomError.ErrKeyDoesNotExist
	}
	d.Manifest.SegmentLevels[level].Mu.Lock()
	sz := len(d.Manifest.SegmentLevels[level].Segments)
	d.Manifest.SegmentLevels[level].Mu.Unlock()
	if sz <= segmentIndex {
		return d.CheckALevelForAKey(key, level, sz-1)
	}

	d.Manifest.SegmentLevels[level].Mu.Lock()
	l.Infof("Attempting to check segment file %d for key %s", d.Manifest.SegmentLevels[level].Segments[segmentIndex].SegmentId, key)
	memtable := memtable.GetNewMemTable(d.Manifest.DbName, -1) // passing -1 cuz segmentId will be updated in the next line
	l.Debugln(d.Manifest.SegmentLevels[level].Segments[segmentIndex])
	memtable.LoadFromSegmentFile(d.Manifest.SegmentLevels[level].Segments[segmentIndex].SegmentId)
	d.Manifest.SegmentLevels[level].Mu.Unlock()

	value, err := memtable.Get(key)
	l.Debugf("Got value :%s,%v", value, err)
	if err != nil && errors.Is(err, CustomError.ErrKeyDoesNotExist) {
		// check before segment file recursively
		return d.CheckALevelForAKey(key, level, segmentIndex-1)
	}

	return value, nil
}

// Returns the most recent segment id plus 1 from the disk store
func (d *DiskStore) GetNewSegmentId() uint32 {
	d.Manifest.Mu.Lock()
	defer func() {
		d.Manifest.Mu.Unlock()
	}()
	d.Manifest.MaxSegmentId += 1
	return d.Manifest.MaxSegmentId
}

// clears the db
func (d *DiskStore) Cleanup() {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "Cleanup",
	})
	l.Infoln("Cleaning up the database")

	// wait for any memtable disk writes to finish
	if d.AuxillaryMemtable != nil {
		l.Infoln("Waiting for aux memtable write to disk to finish")
		d.AuxillaryMemtable.ExWaitGroup.Mu.Lock()
		d.AuxillaryMemtable.ExWaitGroup.Wg.Wait()
		d.AuxillaryMemtable.ExWaitGroup.Mu.Unlock()

	}

	// wait for merge compactor process
	d.MergeCompactorWg.Wait()

	// clear the segments slice
	d.Manifest.Mu.Lock()
	d.Manifest.NumberOfLevels = 0
	d.Manifest.MaxSegmentId = 1
	// segment levels maybe locked in merge compaction
	d.Manifest.SegmentLevels = []SegmentLevelMetadata{}

	d.Memtable = memtable.GetNewMemTable(d.Manifest.DbName, 1)
	d.AuxillaryMemtable = nil
	d.HashIndex = HashIndex{}
	d.MergeCompactor = []MergeCompactor{}

	// delete everything including manifest file

	path := config.Config.Path
	dirPath := fmt.Sprintf("%s/%s", path, d.Manifest.DbName)
	d.Manifest.Mu.Unlock()

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

	d.Manifest.Mu.Lock()
	defer func() {
		d.Manifest.Mu.Unlock()
	}()

	marshalledManifestData, err := json.Marshal(d.Manifest)
	if err != nil {
		l.Panicf("Error in marshalling  manifest obejct %v", err)
	}

	manifestFile := fmt.Sprintf("%s/%s/manifest.json", config.Config.Path, d.Manifest.DbName)
	// err = encoder.Encode(manifest)
	err = ioutil.WriteFile(manifestFile, marshalledManifestData, 0666)

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

	// wait for any memtable disk writes to finish
	if d.AuxillaryMemtable != nil {
		l.Infoln("Waiting for aux memtable write to disk to finish")
		d.AuxillaryMemtable.ExWaitGroup.Mu.Lock()
		d.AuxillaryMemtable.ExWaitGroup.Wg.Wait()
		d.AuxillaryMemtable.ExWaitGroup.Mu.Unlock()
	}
	d.MergeCompactorWg.Wait()

	// write memtable to segment file and clear it

	// newly created db closed without actually writing to disk
	if d.Manifest.NumberOfLevels == 0 {
		d.Manifest.Mu.Lock()
		// add the new segment level 0 to manifest
		d.Manifest.NumberOfLevels = 1
		d.Manifest.SegmentLevels = append(d.Manifest.SegmentLevels, SegmentLevelMetadata{
			Segments: []SegmentMetadata{},
			Mu:       &sync.Mutex{},
		})
		d.Manifest.Mu.Unlock()
		d.InitMergeCompactor(0)
	}

	cardinality, exists, err := d.Memtable.WriteMemtableToDisk()
	if err != nil {
		l.Fatalf("Error while writing memtable to disk %v", err)
	}
	// append only if its newly added file
	if !exists {
		d.Manifest.Mu.Lock()
		d.Manifest.SegmentLevels[0].Segments = append(d.Manifest.SegmentLevels[0].Segments, SegmentMetadata{
			SegmentId:   uint32(d.Memtable.SegmentId),
			Cardinality: cardinality,
			Mu:          &sync.Mutex{},
		})

		// unlock specifically here because next function locks it
		d.Manifest.Mu.Unlock()
		// only do merge compaction if segment doesnt exist already
		d.WatchLevelForSizeLimitExceed(0)
	} else {
		// just update cardinality
		d.FindForSegmendAndUpdate(uint32(d.Memtable.SegmentId), cardinality)
	}
	d.ChangeNumberOfSegmentsInManifest()
	d.Memtable.Clear()

	// close manifest file
	d.ManifestFile.Close()
}
