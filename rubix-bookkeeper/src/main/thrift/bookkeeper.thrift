namespace java com.qubole.rubix.bookkeeper

typedef i64 long

service BookKeeperService
{
    list<bool> getCacheStatus(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    oneway void setAllCached(1:string remotePath, 2:long fileLength, 3:long lastModified, 4:long startBlock, 5:long endBlock)

    map<string,double> getCacheStats()
}
