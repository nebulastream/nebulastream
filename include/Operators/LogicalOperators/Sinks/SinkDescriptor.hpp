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
    * @brief Checks if the current node is of type SinkMedium
    * @tparam SinkType
    * @return bool true if node is of SinkMedium
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
    virtual std::string toString() = 0;
    virtual bool equal(SinkDescriptorPtr other) = 0;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINK_SINKDESCRIPTOR_HPP_
