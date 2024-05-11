package tinyamodb

type Config struct {
	BaseDir   string
	Partition struct {
		Num uint8
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
	}
}
