package main

const bigChanSize int64 = 1000000

// Some custom storage classes.  The size of the object is examined and then optimized to a real S3 storage class
// E.G. if an object is 128KB or more in size and StorageClassOptimizeIA is passed then the storage class will be set
// to STANDARD_IA.  If it's less than 128KB then the storage class is set to STANDARD.  For Glacier a threshold of 12KB
// is used and for Deep Archive a threshold of 4KB is used based on some rough calcs accomodating for the 40K overhead
// needed for both Glacier storage types
const (
	StorageClassOptimizeIA          = "S3KOR_OPT_IA"
	StorageClassOptimizeGlacier     = "S3KOR_OPT_GLACIER"
	StorageClassOptimizeDeepArchive = "S3KOR_OPT_DEEPARCHIVE"
)

const (
	StorageClassOptimizeIASize          = 128*1024 - 1
	StorageClassOptimizeGlacierSize     = 12*1024 - 1
	StorageClassOptimizeDeepArchiveSize = 4*1024 - 1
)
