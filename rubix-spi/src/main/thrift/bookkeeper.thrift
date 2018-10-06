namespace java com.qubole.rubix.spi.thrift

typedef i64 long
typedef i32 int

enum Location {
CACHED,
LOCAL,
NON_LOCAL
}

struct BlockLocation {
    1: required Location location;
    2: required string remoteLocation;
}

struct FileInfo {
		1: required long fileSize;
		2: required long lastModified;
}

struct CacheStatusRequest {
		1: required string remotePath;
		2: required long fileLength;
		3: required long lastModified;
		4: required long startBlock;
		5: required long endBlock;
		6: required int clusterType;
		7: optional bool incrMetrics = false;
}

service BookKeeperService
{
    list<BlockLocation> getCacheStatus(1:CacheStatusRequest request)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    map<string,double> getCacheStats()

    bool readData(1:string path, 2:long readStart, 3:int length, 4:long fileSize, 5:long lastModified, 6:int clusterType)

    oneway void handleHeartbeat(1:string workerHostname)

    FileInfo getFileInfo(1: string remotePath)
}
