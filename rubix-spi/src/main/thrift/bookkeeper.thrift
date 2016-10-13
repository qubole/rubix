namespace java com.qubole.rubix.spi

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

struct DataRead {
1: binary data
2: i32 sizeRead
}



service BookKeeperService
{
    list<BlockLocation> getCacheStatus(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock, 6:int clusterType)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    map<string,double> getCacheStats()

    DataRead readData(1:string path, 2:long readStart, 3:int offset, 4:int length)
}
