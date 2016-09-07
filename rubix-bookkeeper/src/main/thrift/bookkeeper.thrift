namespace java com.qubole.rubix.bookkeeper

typedef i64 long
typedef i32 int

service BookKeeperService
{
    list<int> getCacheStatus(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock, 6:int cluster)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    map<string,double> getCacheStats()
}
