#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_

#include <Util/Logger.hpp>
#include <iostream>
#include <memory>

namespace NES {

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

/**
 * @brief This class is used for representing the description of a sink operator
 */
class SinkDescriptor : public std::enable_shared_from_this<SinkDescriptor> {

  public:
    SinkDescriptor(){};

    virtual ~SinkDescriptor() = default;

    /**
    * @brief Checks if the current node is of type SinkType
    * @tparam SinkType
    * @return bool true if node is of SinkType
    */
    template<class SinkType>
    bool instanceOf() {
        if (dynamic_cast<SinkType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a NodeType
    * @tparam NodeType
    * @return returns a shared pointer of the NodeType
    */
    template<class SinkType>
    std::shared_ptr<SinkType> as() {
        if (instanceOf<SinkType>()) {
            return std::dynamic_pointer_cast<SinkType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
