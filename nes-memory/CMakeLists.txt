add_library(nes-memory
        BufferManager.cpp
        TupleBufferImpl.cpp
        TupleBuffer.cpp
        NesDefaultMemoryAllocator.cpp
        FixedSizeBufferPool.cpp
        LocalBufferPool.cpp
        TaggedPointer.cpp
)

target_include_directories(nes-memory
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
find_package(folly REQUIRED)
target_link_libraries(nes-memory PUBLIC nes-common PRIVATE folly::folly)

### What I want:
#
# target_link_libraries(nes-executable
#         PUBLIC nes-memory nes-source-sink-descriptor
# )
install(TARGETS nes-memory
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)
