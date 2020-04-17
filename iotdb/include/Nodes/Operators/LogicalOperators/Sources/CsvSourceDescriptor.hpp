#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class CsvSourceDescriptor : public SourceDescriptor {

  public:

    CsvSourceDescriptor(std::string filePath, std::string& delimiter, size_t numBuffersToProcess,
                        size_t frequency);

    SourceDescriptorType getType() override;

    const std::string& getFilePath() const;
    const std::string& getDelimiter() const;
    size_t getNumBuffersToProcess() const;
    size_t getFrequency() const;

  private:

    std::string filePath;
    std::string delimiter;
    size_t numBuffersToProcess;
    size_t frequency;
};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
