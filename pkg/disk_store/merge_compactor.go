package disk_store

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	CustomError "github.com/abesheknarayan/go-caskdb/pkg/error"
	"github.com/abesheknarayan/go-caskdb/pkg/key_entry"
	"github.com/abesheknarayan/go-caskdb/pkg/memtable"
	"github.com/abesheknarayan/go-caskdb/pkg/utils"
	"github.com/sirupsen/logrus"
)

// this struct is maintained for each level
type MergeCompactor struct {
	Mu *sync.Mutex
}

func (d *DiskStore) InitMergeCompactor(level uint32) {
	// apending to disk store object
	d.MergeCompactor = append(d.MergeCompactor, MergeCompactor{
		Mu: &sync.Mutex{},
	})
}

// watches the particular manifest level every X(configurable) seconds and initiates compaction strategy if necessary
func (d *DiskStore) WatchLevelForSizeLimitExceed(level uint32) {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "WatchLevelForSizeLimitExceed",
	})

	// check for size exceeded case
	d.Manifest.Mu.Lock()
	if len(d.Manifest.SegmentLevels) <= int(level) {
		d.Manifest.Mu.Unlock()
		return
	}
	sizeOfCurrentLevel := len(d.Manifest.SegmentLevels[level].Segments)
	d.Manifest.Mu.Unlock()

	if sizeOfCurrentLevel > int(MaxSizeForLevel(level)) {
		l.Debugln("Sending true to level", level)
		err := d.AddSegmentToLevelAndPerformCompaction(level + 1)

		if err != nil {
			l.Errorln(err)
		}
	}
}

// This function takes the file with the passed segment id and merges with passed level
func (d *DiskStore) AddSegmentToLevelAndPerformCompaction(nextLevel uint32) error {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "AddSegmentToLevelAndPerformCompaction",
	})
	l.Infof("Attempting to perform merge compaction from level %d to level %d", nextLevel-1, nextLevel)

	currentLevel := nextLevel - 1

	d.MergeCompactorWg.Add(1)

	d.Manifest.Mu.Lock()

	// check if next level exists
	if len(d.MergeCompactor) <= int(nextLevel) {
		// initiate next level
		d.Manifest.NumberOfLevels += 1
		d.Manifest.SegmentLevels = append(d.Manifest.SegmentLevels, SegmentLevelMetadata{
			Segments: []SegmentMetadata{},
			Mu:       &sync.Mutex{},
		})
		d.InitMergeCompactor(nextLevel)
	}

	sz := len(d.Manifest.SegmentLevels[currentLevel].Segments)
	if sz > 0 {
		// pop the first segment
		d.Manifest.SegmentLevels[currentLevel].Mu.Lock()
		leastRecentSegmentOnCurrentLevel := d.Manifest.SegmentLevels[currentLevel].Segments[0]
		d.Manifest.SegmentLevels[currentLevel].Mu.Unlock()
		d.Manifest.Mu.Unlock()
		/*
		  - merging all segments from smaller to bigger
		*/
		err := d.MergeCompact(leastRecentSegmentOnCurrentLevel, nextLevel)
		l.Infoln("Finished merging onto level", nextLevel, "from level ", nextLevel-1)
		d.MergeCompactorWg.Done()

		if err != nil {
			return err
		}

		// trigger everytime an insertion at next level happens
		d.Manifest.SegmentLevels[currentLevel].Mu.Lock()
		d.Manifest.SegmentLevels[currentLevel].Segments = d.Manifest.SegmentLevels[currentLevel].Segments[1:]
		d.Manifest.SegmentLevels[currentLevel].Mu.Unlock()

		go d.WatchLevelForSizeLimitExceed(nextLevel)
	} else {

		d.Manifest.SegmentLevels[currentLevel].Mu.Unlock()
		d.Manifest.Mu.Unlock()
		return CustomError.ErrSegmentLevelEmpty
	}

	return nil
}

// performs merge compaction of segment onto level `level`
func (d *DiskStore) MergeCompact(mergingSegment SegmentMetadata, level uint32) error {
	var l = utils.Logger.WithFields(logrus.Fields{
		"method": "MergeCompact",
	})
	l.Infof("Attempting to merge segment %d.seg to level %d", mergingSegment.SegmentId, level)

	var allSegments []SegmentMetadata

	d.Manifest.Mu.Lock()
	d.Manifest.SegmentLevels[level].Mu.Lock()
	l.Debugln("here 1")
	defer func() {
		d.Manifest.SegmentLevels[level].Mu.Unlock()
		d.Manifest.Mu.Unlock()
	}()

	allSegments = append(allSegments, mergingSegment)
	allSegments = append(allSegments, d.Manifest.SegmentLevels[level].Segments...)

	// sort `allSegments` to lowest cardinality first order
	sort.Slice(allSegments, func(i, j int) bool {
		return allSegments[i].SegmentId > allSegments[j].SegmentId
	})

	// one big memtable :o
	mergedMemtable := memtable.MemTable{
		DbName:        d.Manifest.DbName,
		BytesOccupied: 0,
		Map:           &memtable.HashMap{M: make(map[string]key_entry.KeyEntry), Mu: &sync.Mutex{}},
		Mu:            &sync.Mutex{},
		Wg:            &sync.WaitGroup{},
	}

	for _, segment := range allSegments {
		tempMemtable := memtable.MemTable{
			DbName:        d.Manifest.DbName,
			BytesOccupied: 0,
			Map:           &memtable.HashMap{M: make(map[string]key_entry.KeyEntry), Mu: &sync.Mutex{}},
			Mu:            &sync.Mutex{},
			Wg:            &sync.WaitGroup{},
		}
		err := tempMemtable.LoadFromSegmentFile(segment.SegmentId)
		if err != nil {
			return fmt.Errorf("error while performing merge compaction of segment %d onto level %d", segment.SegmentId, level)
		}
		for key, keyEntry := range tempMemtable.Map.M {
			if mergedMemtable.Contains(key) {
				// // insert only if key_entry from merged memtable is older
				// mergedKeyEntry := mergedMemtable.Map.M[key]
				// l.Debugln(mergedKeyEntry.Timestamp, keyEntry.Timestamp)
				// if mergedKeyEntry.Timestamp < keyEntry.Timestamp {
				// 	// change value and timestamp in mergedMemtable

				// 	// note not using `Put` method here to prevent the size exceeded error
				// 	mergedMemtable.Map.M[key] = keyEntry
				// }
			} else {
				mergedMemtable.Map.M[key] = keyEntry
			}
		}
	}

	// split everything into multiple files
	// make sure of giving proper segment ids
	// number of merged segemnts <= already present segments
	var segmentIds []uint32

	for _, segment := range d.Manifest.SegmentLevels[level].Segments {
		segmentIds = append(segmentIds, segment.SegmentId)
	}
	segmentIds = append(segmentIds, mergingSegment.SegmentId)

	segmendIndex := len(segmentIds) - 1

	// using temporary Memtable
	tempMemtable := memtable.MemTable{
		DbName:        d.Manifest.DbName,
		SegmentId:     int32(segmentIds[segmendIndex]),
		BytesOccupied: 0,
		Map:           &memtable.HashMap{M: make(map[string]key_entry.KeyEntry), Mu: &sync.Mutex{}},
		Mu:            &sync.Mutex{},
		Wg:            &sync.WaitGroup{},
	}

	// delete all segments already present

	for _, id := range segmentIds {
		err := utils.DeleteFile(fmt.Sprintf("%s/%s/%d.seg", config.Config.Path, d.Manifest.DbName, id))
		if err != nil {
			return fmt.Errorf("error while deleting file %d.seg: %v", id, err)
		}
	}

	// clear manifest level
	d.Manifest.SegmentLevels[level].Segments = d.Manifest.SegmentLevels[level].Segments[:0]

	for key, keyEntry := range mergedMemtable.Map.M {
		err := tempMemtable.Put(key, keyEntry.Value)
		if err == CustomError.ErrMaxSizeExceeded {
			cardinality, _, err := tempMemtable.WriteMemtableToDisk()
			if err != nil {
				return fmt.Errorf("error while writing temporary memtable to disk")
			}
			l.Infof("Successfully written the temporary memtable to disk with cardinality: %d", cardinality)

			// add into manifest level
			d.Manifest.SegmentLevels[level].Segments = append(d.Manifest.SegmentLevels[level].Segments, SegmentMetadata{
				SegmentId:   uint32(tempMemtable.SegmentId),
				Cardinality: cardinality,
				Mu:          &sync.Mutex{},
			})

			// update memtable for next step
			tempMemtable.Clear()
			segmendIndex--
			tempMemtable.SegmentId = int32(segmentIds[segmendIndex])

			// put again, this time there wont be any error
			tempMemtable.Put(key, keyEntry.Value)
		}
	}

	// write leftover temp memtable elements onto disk
	if tempMemtable.BytesOccupied > 0 {
		cardinality, _, err := tempMemtable.WriteMemtableToDisk()
		if err != nil {
			return fmt.Errorf("error while writing temporary memtable to disk")
		}
		l.Infof("Successfully written the temporary memtable to disk with cardinality: %d\n", cardinality)

		// add into manifest level
		d.Manifest.SegmentLevels[level].Segments = append(d.Manifest.SegmentLevels[level].Segments, SegmentMetadata{
			SegmentId:   uint32(tempMemtable.SegmentId),
			Cardinality: cardinality,
			Mu:          &sync.Mutex{},
		})
	}

	l.Infof("Merge Compaction of level %d is complete!!\n", level)

	return nil
}

func MaxSizeForLevel(level uint32) uint64 {
	return uint64(math.Pow(10, float64(level)))
}
