/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_SOURCES_TABLE_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_TABLE_SOURCE_HPP_

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>
namespace NES {
class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;
namespace Runtime {
namespace detail {
class MemorySegment;
}
}// namespace Runtime
/**
 * @brief Table Source
 * todo Still under development
 */
class TableSource : public GeneratorSource, public Runtime::BufferRecycler {
  public:
    /**
     * @brief The constructor of a TableSource
     * @param // todo
     * @param operatorId the valid id of the source
     */
    explicit TableSource(SchemaPtr schema,
                          std::string pathTableFile,
                          Runtime::BufferManagerPtr bufferManager,
                          Runtime::QueryManagerPtr queryManager,
                          OperatorId operatorId,
                          size_t numSourceLocalBuffers,
                          std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);
    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

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

    virtual void recyclePooledBuffer(Runtime::detail::MemorySegment*) override{};

    /**
     * @brief Interface method for unpooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override{};

  private:
    std::string pathTableFile;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::shared_ptr<uint8_t> memoryArea;
    size_t memoryAreaSize;
    uint64_t currentPositionInBytes;
    uint64_t tupleSizeInBytes;
    uint64_t bufferSize;
    size_t numTuples; // in table

    void fillBuffer(Runtime::TupleBuffer &buffer);
};

using TableSourcePtr = std::shared_ptr<TableSource>;

}// namespace NES

#endif// NES_INCLUDE_SOURCES_TABLE_SOURCE_HPP_
