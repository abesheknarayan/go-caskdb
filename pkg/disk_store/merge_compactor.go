package disk_store

import (
	"math"
	"sync"

	CustomError "github.com/abesheknarayan/go-caskdb/pkg/error"
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

	// l.Debugln("Listening in level", level)

	// TODO
	// check for size exceeded case
	d.Manifest.Mu.Lock()
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

	d.MergeCompactorWg.Add(1)

	defer func() {
		l.Infoln("Finished merging onto level", nextLevel, "from level ", nextLevel-1)
		d.MergeCompactorWg.Done()
	}()

	// TODO

	// check if next level exists
	l.Debugln(len(d.MergeCompactor), nextLevel, d.MergeCompactor)
	if len(d.MergeCompactor) <= int(nextLevel) {
		// initiate next level
		d.Manifest.NumberOfLevels += 1
		d.Manifest.SegmentLevels = append(d.Manifest.SegmentLevels, SegmentLevelMetadata{
			Segments: []SegmentMetadata{},
			Mu:       &sync.Mutex{},
		})
		d.InitMergeCompactor(nextLevel)
	}

	// take the segment id and push it to next level
	// remove segmend id from nextlevel-1 's manifest
	// add it to nextlevel's manifest for now (later do the actual merge compaction strategy)
	// typically the SegmentId is the first element of prev level (nextLevel-1)

	currentLevel := nextLevel - 1
	d.Manifest.SegmentLevels[nextLevel].Mu.Lock()
	d.Manifest.SegmentLevels[currentLevel].Mu.Lock()

	defer func() {
		d.Manifest.SegmentLevels[nextLevel].Mu.Unlock()
		d.Manifest.SegmentLevels[currentLevel].Mu.Unlock()
	}()
	sz := len(d.Manifest.SegmentLevels[currentLevel].Segments)
	// l.Debugln(d.Manifest.SegmentLevels)
	// l.Debugln(nextLevel, currentLevel)
	var leastRecentSegmentOnCurrentLevel SegmentMetadata
	if sz > 0 {
		// pop the first segment
		leastRecentSegmentOnCurrentLevel = d.Manifest.SegmentLevels[currentLevel].Segments[0]
		d.Manifest.SegmentLevels[currentLevel].Segments = d.Manifest.SegmentLevels[currentLevel].Segments[1:]
	} else {
		return CustomError.ErrSegmentLevelEmpty
	}
	// l.Debugln(sz, leastRecentSegmentOnCurrentLevel)

	// TODO: for now just adding, later apply proper merge compaction strategy here
	d.Manifest.SegmentLevels[nextLevel].Segments = append(d.Manifest.SegmentLevels[nextLevel].Segments, leastRecentSegmentOnCurrentLevel)
	l.Debugln("here", leastRecentSegmentOnCurrentLevel, d.Manifest)

	// trigger everytime an insertion at next level happens
	go d.WatchLevelForSizeLimitExceed(nextLevel)

	return nil
}

func MaxSizeForLevel(level uint32) uint64 {
	return uint64(math.Pow(10, float64(level)))
}
