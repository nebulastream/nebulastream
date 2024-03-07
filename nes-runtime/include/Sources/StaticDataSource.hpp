/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_RUNTIME_INCLUDE_SOURCES_STATICDATASOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_STATICDATASOURCE_HPP_

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>
#include <fstream>

namespace NES {

class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;

namespace Runtime::detail {
class MemorySegment;
}// namespace Runtime::detail

namespace Experimental {
/**
 * @brief Table Source
 * todo Still under development
 */
class StaticDataSource : public GeneratorSource, public ::NES::Runtime::BufferRecycler {
  public:
    /**
     * @brief The constructor of a StaticDataSource
     * @param schema the schema of the data
     * @param pathTableFile
     * @param lateStart wether to start the source late
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     */
    explicit StaticDataSource(SchemaPtr schema,
                              std::string pathTableFile,
                              const bool lateStart,
                              ::NES::Runtime::BufferManagerPtr bufferManager,
                              ::NES::Runtime::QueryManagerPtr queryManager,
                              OperatorId operatorId,
                              OriginId originId,
                              StatisticId statisticId,
                              size_t numSourceLocalBuffers,
                              const std::string& physicalSourceName,
                              std::vector<::NES::Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief overwrite DataSource::start(). Only start runningRoutine, if lateStart==false.
     */
    bool start() final;

    /**
     * @brief method to start the source, calls DataSource::start().
     * 1.) check if bool running is true, if true return if not start source
     * 2.) start new thread with runningRoutine
     */
    bool startStaticDataSourceManually();

    /**
     * @brief API method called upon receiving an event. At startSourceEvent, start source.
     * @param event
     */
    void onEvent(Runtime::BaseEvent&) final;

    void open() override;

    void preloadBuffers();

    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<::NES::Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief Provides a string representation of the source
     * @return The string representation of the source
     */
    std::string toString() const override;

    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override;

    virtual void recyclePooledBuffer(::NES::Runtime::detail::MemorySegment*) override{};

    /**
     * @brief Interface method for unpooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recycleUnpooledBuffer(::NES::Runtime::detail::MemorySegment*) override{};

  private:
    /* message & late start system */
    bool lateStart;
    bool startCalled = false;
    mutable std::mutex startConfigMutex;

    /* preloading */
    bool eagerLoading = true;
    bool onlySendDataWhenLoadingIsFinished = true;

    /* file parsing */
    bool fileEnded = false;
    std::ifstream input;
    CSVParserPtr inputParser;
    uint64_t currentPositionInFile{0};
    std::vector<Runtime::TupleBuffer> filledBuffers;

    std::string pathTableFile;
    uint64_t numTuplesPerBuffer;
    uint64_t tupleSizeInBytes;
    uint64_t bufferSize;

    size_t numTuples;// in table
    size_t numTuplesEmitted = 0;
    size_t numBuffersEmitted = 0;

    void fillBuffer(::NES::Runtime::MemoryLayouts::DynamicTupleBuffer& buffer);
};

using StaticDataSourcePtr = std::shared_ptr<StaticDataSource>;

}// namespace Experimental
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_STATICDATASOURCE_HPP_
