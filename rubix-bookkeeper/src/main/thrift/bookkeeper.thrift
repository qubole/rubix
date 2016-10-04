namespace java com.qubole.rubix.bookkeeper

typedef i64 long
typedef i32 int

enum Location {
CACHED,
LOCAL,
NON_LOCAL
}


service BookKeeperService
{
    list<Location> getCacheStatus(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock, 6:int clusterType)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    map<string,double> getCacheStats()
}
