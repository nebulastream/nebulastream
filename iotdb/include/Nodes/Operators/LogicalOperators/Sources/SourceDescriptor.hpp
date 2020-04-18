#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <iostream>
#include <memory>
#include <API/Schema.hpp>

namespace NES {

enum SourceDescriptorType {
    ZmqSource, SenseSource, KafkaSource, CsvSource,  BinarySource, DefaultSource
};

class SourceDescriptor {

  public:

    SourceDescriptor(SchemaPtr schema) : schema(schema) {};

    virtual SourceDescriptorType getType() = 0;

    const SchemaPtr getSchema() const {
        return schema;
    }

  private:
    SchemaPtr schema;
};

typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
