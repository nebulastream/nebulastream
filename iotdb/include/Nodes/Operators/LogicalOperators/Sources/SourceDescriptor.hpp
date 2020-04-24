#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <iostream>
#include <memory>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

namespace NES {

enum SourceDescriptorType {
    ZmqSource, SenseSource, KafkaSource, CsvSource,  BinarySource, DefaultSource, LogicalStreamSource
};

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SourceDescriptor : public std::enable_shared_from_this<SourceDescriptor>{

  public:

    SourceDescriptor(SchemaPtr schema) : schema(schema) {};

    virtual SourceDescriptorType getType() = 0;

    const SchemaPtr getSchema() const {
        return schema;
    }

    /**
    * @brief Checks if the current node is of type SourceType
    * @tparam SourceType
    * @return bool true if node is of SourceType
    */
    template<class SourceType>
    const bool instanceOf() {
        if (dynamic_cast<SourceType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the node to a SourceType
    * @tparam SourceType
    * @return returns a shared pointer of the SourceType
    */
    template<class SourceType>
    std::shared_ptr<SourceType> as() {
        if (instanceOf<SourceType>()) {
            return std::dynamic_pointer_cast<SourceType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("We performed an invalid cast");
            throw std::bad_cast();
        }
    }

  private:
    SchemaPtr schema;
};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
