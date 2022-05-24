//
// Created by eleicha on 20.05.22.
//

#ifndef NES_INCLUDE_SOURCES_TENSORSOURCE_HPP
#define NES_INCLUDE_SOURCES_TENSORSOURCE_HPP

#include <Catalogs/Source/PhysicalSourceTypes/TensorSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <fstream>

namespace NES{

class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;
/**
 * @brief this class implement the CSV as an input source
 */
class TensorSource : public DataSource {
  public:
    /**
   * @brief constructor of CSV source
   * @param schema of the source
   * @param bufferManager the buffer manager
   * @param queryManager the query manager
   * @param csvSourceType points to the current source configuration object, look at mqttSourceType and CSVSourceType for info
   * @param delimiter inside the file, default ","
   * @param operatorId current operator id
   * @param numSourceLocalBuffers
   * @param gatheringMode
   * @param successors
   */
    explicit TensorSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       TensorSourceTypePtr tensorSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer&);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get file path for the csv file
     */
    std::string getFilePath() const;

    /**
     * @brief Get the csv file delimiter
     */
    std::string getDelimiter() const;

    /**
     * @brief Get number of tuples per buffer
     */
    uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief getter for skip header
     */
    bool getSkipHeader() const;

    /**
     * @brief getter for source config
     * @return csvSourceType1
     */
    const TensorSourceTypePtr& getSourceConfig() const;

  protected:
    std::ifstream input;
    bool fileEnded;
    bool loopOnFile;

  private:
    TensorSourceTypePtr tensorSourceType;
    uint64_t currentPositionInFile{0};
    std::vector<PhysicalTypePtr> physicalTypes;
    size_t fileSize;
    uint64_t tupleSize;
    CSVParserPtr inputParser;
};

using TensorSourcePtr = std::shared_ptr<TensorSource>;

}

#endif//NES_INCLUDE_SOURCES_TENSORSOURCE_HPP
