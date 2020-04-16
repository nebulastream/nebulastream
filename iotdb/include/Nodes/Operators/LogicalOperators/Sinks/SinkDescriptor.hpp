#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_

#include <iostream>
#include <memory>

namespace NES {


enum SinkDescriptorType {
    FileDescriptor, KafkaDescriptor
};

/**
 * @brief This class is used for representing the description of a sink operator
 */
class SinkDescriptor {

  public:
    virtual SinkDescriptorType getType() = 0;
};

typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
