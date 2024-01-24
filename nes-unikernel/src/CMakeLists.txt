add_source_files(unikernel-lib
#        DataSource.cpp
        NetworkSink.cpp
        SinkMedium.cpp
#        Util/MMapCircularBuffer.cpp
        Util/CircularBuffer.cpp
        Util/StacktraceLoader.cpp
#        Sources/TCPSource.cpp
)

add_subdirectory(proxy)