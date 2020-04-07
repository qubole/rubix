namespace java com.qubole.rubix.spi.thrift

typedef i32 int

struct Request {
    1: required string message;
    2: required int sleepSecs;
}

service TestingService {
    string echo(1: Request request)
}
