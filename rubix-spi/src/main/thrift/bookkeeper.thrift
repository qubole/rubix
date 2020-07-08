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

struct HeartbeatStatus {
    1: required bool fileValidationSucceeded;
    2: required bool cachingValidationSucceeded;
}

struct CacheStatusRequest {
		1: required string remotePath;
		2: required long fileLength;
		3: required long lastModified;
		4: required long startBlock;
		5: required long endBlock;
		6: optional int clusterType;
		7: optional bool incrMetrics = false;
}

struct CacheStatusResponse {
        1: required list<BlockLocation> blocks;
        2: required int generationNumber;
}

struct ReadResponse {
        1: required bool status;
        2: required int generationNumber;
}

service BookKeeperService
{
    CacheStatusResponse getCacheStatus(1:CacheStatusRequest request)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock, 6:int generationNumber)

    map<string,double> getCacheMetrics()

    ReadResponse readData(1:string path, 2:long readStart, 3:int length, 4:long fileSize, 5:long lastModified, 6:int clusterType)

    oneway void handleHeartbeat(1:string workerHostname, 2:HeartbeatStatus heartbeatStatus)

    FileInfo getFileInfo(1: string remotePath)

    bool isBookKeeperAlive()

    oneway void invalidateFileMetadata(1:string remotePath)
}
