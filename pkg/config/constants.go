package config

const NUMBER_OF_SEGMENTS_FOR_MERGE_COMPACTION uint32 = 5 // random, configurable
const COMPACTION_WATCH_FREQUENCY uint32 = 10             // sleeps and watches every X milliseconds to see if size of current level has increased to initiate merge compaction
