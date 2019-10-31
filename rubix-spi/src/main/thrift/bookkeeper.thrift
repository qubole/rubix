namespace java com.qubole.rubix.spi.thrift

typedef i64 long
typedef i32 int

enum Location {
    CACHED,
    LOCAL,
    NON_LOCAL,
    UNKNOWN
}

enum NodeState {
    ACTIVE,
    INACTIVE
}

struct ClusterNode {
    1: required string nodeUrl;
    2: required NodeState nodeState;
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
	6: optional bool incrMetrics = false;
}

struct ReadDataRequest {
    1: required string remotePath;
    2: required long readStart;
    3: required long readLength;
    4: required long fileSize;
    5: required long lastModified;
}

struct SetCachedRequest {
    1: required string remotePath;
    2: required long fileSize;
    3: required long lastModified;
    4: required long startBlock;
    5: required long endBlock;
}

struct HeartbeatRequest {
    1: required string workerHostname;
    2: required HeartbeatStatus heartbeatStatus;
}

service BookKeeperService
{
    list<BlockLocation> getCacheStatus(1:CacheStatusRequest request)

    oneway void setAllCached(1:SetCachedRequest request)

    map<string,double> getCacheMetrics()

    bool readData(1:ReadDataRequest request)

    oneway void handleHeartbeat(1:HeartbeatRequest request)

    FileInfo getFileInfo(1:string remotePath)

    list<ClusterNode> getClusterNodes()

    string getOwnerNodeForPath(1:string remotePathKey)

    bool isBookKeeperAlive()

    oneway void invalidateFileMetadata(1:string remotePath)
}
