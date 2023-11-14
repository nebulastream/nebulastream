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

#ifndef NES_CORE_INCLUDE_SOURCES_ARROWSOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_ARROWSOURCE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/ArrowSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/Arrow/ArrowSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <arrow/csv/api.h>
#include <arrow/csv/type_fwd.h>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/type_fwd.h>
#include <arrow/type_fwd.h>
#include <string>

namespace NES {

class ArrowParser;
using ArrowParserPtr = std::shared_ptr<ArrowParser>;

/**
 * @brief this class implement the Arrow as an input source
 * @brief more specifically we currently support the Arrow IPC format (a.k.a Feather v2)
 * @see https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
 */
class ArrowSource : public DataSource {
  public:
    /**
    * @brief constructor of Arrow source
    * @param schema of the source
    * @param bufferManager the buffer manager
    * @param queryManager the query manager
    * @param arrowSourceType points to the current source configuration object, look at mqttSourceType for info
    * @param operatorId current operator id
    * @param numSourceLocalBuffers
    * @param gatheringMode
    * @param successors
    */
    explicit ArrowSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         ArrowSourceTypePtr arrowSourceType,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         GatheringMode gatheringMode,
                         const std::string& physicalSourceName,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief override the receiveData method for the arrow source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer&);

    /**
     * @brief override the toString method for the Arrow source
     * @return returns string describing the Arrow source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get file path for the Arrow file
     */
    std::string getFilePath() const;

    /**
     * @brief getter for source config
     * @return arrowSourceType1
     */
    const ArrowSourceTypePtr& getSourceConfig() const;

  protected:
    bool fileEnded;

  private:
    ArrowSourceTypePtr arrowSourceType;
    std::string filePath;
    bool skipHeader;
    std::string delimiter;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::vector<PhysicalTypePtr> physicalTypes;
    size_t fileSize;
    ArrowParserPtr inputParser;

    // arrow related data structures and helper functions
    // TODO #4083: these should move to an ArrowWrapper when we support other formats from Arrow
    // Arrow status returns at every operation also do not play well currently
    std::shared_ptr<arrow::io::ReadableFile> inputFile;
    // A record batch in Arrow is two-dimensional data structure that is semantically a sequence
    // of fields, each a contiguous Arrow array
    // See: https://arrow.apache.org/docs/cpp/api/table.html#_CPPv4N5arrow11RecordBatchE
    // At any point in time that we read a record batch from the RecordBatchStreamReader, we maintain
    // it in currentRecordBatch and subsequently transfer the records from it to the DynamicTupleBuffer
    std::shared_ptr<arrow::RecordBatch> currentRecordBatch;
    // this keep track of the last record read from the currentRecordBatch
    uint64_t indexWithinCurrentRecordBatch{0};
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> recordBatchStreamReader;

    // CSV reading from arrow
    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;

    // arrow related utility functions
    /**
     * @brief opens the Arrow inputSource, and initializes the recordBatchStreamReader
     * @return returns an arrow status
     */
    arrow::Status openFile();

    /**
     * @brief opens the csv file and initializes the csv Table reader
     * @return returns an arrow status
     */
    arrow::Status openCsvFile();

    /**
     * Iterate over schema fields and use equivalent arrow types.
     * Arrow will not infer the types itself this way.
     * @return
     */
    void populateOptionsFromSchema(arrow::csv::ReadOptions& readOptions, arrow::csv::ConvertOptions& convertOptions);

    /**
     * @brief reads the next record batch in the recordBatchStreamReader
     * @return returns an arrow status
     */
    void readNextBatch();

    /**
     * @brief this function writes the data from the record batches to DynamicTupleBuffer
     * @param tupleCountInBuffer is the count of total filled buffers in tupleBuffer
     * @param tupleBuffer the tuple buffer to be written
     * @param recordBatch the arrow record batch to be written to tupleBuffer
     * @return returns true if success
     */
    void writeRecordBatchToTupleBuffer(uint64_t tupleCount,
                                       Runtime::MemoryLayouts::DynamicTupleBuffer& buffer,
                                       std::shared_ptr<arrow::RecordBatch> recordBatch);

    /**
     * @brief this function writes the data from the record batches to DynamicTupleBuffer
     * @param tupleCountInBuffer is the count of total filled buffers in tupleBuffer
     * @param schemaFieldIndex the column to be written
     * @param tupleBuffer the tuple buffer to be written
     * @param arrowArray the arrow array to write to the tupleBuffer
     * @return returns true if success
     */
    void writeArrowArrayToTupleBuffer(uint64_t tupleCountInBuffer,
                                      uint64_t schemaFieldIndex,
                                      Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                      const std::shared_ptr<arrow::Array> arrowArray);
};

using ArrowSourcePtr = std::shared_ptr<ArrowSource>;
}// namespace NES

#endif// NES_CORE_INCLUDE_SOURCES_ARROWSOURCE_HPP_
