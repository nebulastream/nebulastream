#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical CSV source
 */
class CsvSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string filePath,
                                      std::string delimiter,
                                      size_t numBuffersToProcess,
                                      size_t numberOfTuplesToProducePerBuffer,
                                      size_t frequency,
                                      bool endlessRepeat);

    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string streamName,
                                      std::string filePath,
                                      std::string delimiter,
                                      size_t numberOfTuplesToProducePerBuffer,
                                      size_t numBuffersToProcess,
                                      size_t frequency,
                                      bool endlessRepeat);

    /**
     * @brief get file path for reading the csv file
     */
    const std::string& getFilePath() const;

    /**
     * @brief get delimiter for the csv file
     */
    const std::string& getDelimiter() const;

    /**
     * @brief Get number of buffers to process
     */
    size_t getNumBuffersToProcess() const;

    /**
    * @brief Get number of number of tuples within the buffer
    */
    size_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the frequency of reading the csv file
     */
    size_t getFrequency() const;
    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

    /**
     * @brief getter/setter endlessRepeat
     * @return
     */
    bool isEndlessRepeat() const;
    void setEndlessRepeat(bool endlessRepeat);

  private:
    explicit CsvSourceDescriptor(SchemaPtr schema,
                                 std::string filePath,
                                 std::string delimiter,
                                 size_t numberOfTuplesToProducePerBuffer,
                                 size_t numBuffersToProcess,
                                 size_t frequency,
                                 bool endlessRepeat);

    explicit CsvSourceDescriptor(SchemaPtr schema,
                                 std::string streamName,
                                 std::string filePath,
                                 std::string delimiter,
                                 size_t numberOfTuplesToProducePerBuffer,
                                 size_t numBuffersToProcess,
                                 size_t frequency,
                                 bool endlessRepeat);
    std::string filePath;
    std::string delimiter;
    size_t numBuffersToProcess;
    size_t numberOfTuplesToProducePerBuffer;
    size_t frequency;
    bool endlessRepeat;
};

typedef std::shared_ptr<CsvSourceDescriptor> CsvSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
