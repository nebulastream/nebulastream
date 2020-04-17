#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_

#include <iostream>
#include <memory>
#include <API/Schema.hpp>

namespace NES {

enum SinkDescriptorType {
    FileDescriptor, KafkaDescriptor, ZmqDescriptor, PrintDescriptor
};

/**
 * @brief This class is used for representing the description of a sink operator
 */
class SinkDescriptor {

  public:

    SinkDescriptor(SchemaPtr schema) : schema(schema) {};

    virtual SinkDescriptorType getType() = 0;

    const SchemaPtr getSchema() const {
        return schema;
    }

  private :

    SchemaPtr schema;
};

typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
