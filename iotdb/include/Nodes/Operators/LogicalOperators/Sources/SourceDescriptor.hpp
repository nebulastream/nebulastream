#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <iostream>
#include <memory>

namespace NES {

enum SourceDescriptorType {
    ZmqDescriptor, KafkaDescriptor, SenseDescriptor, CsvDescriptor,  BinaryDescriptor, DefaultDescriptor
};

class SourceDescriptor {

  public:
    virtual SourceDescriptorType getType();
};

typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
