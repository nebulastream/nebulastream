#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>

namespace NES {

enum SourceDescriptorType {
    ZmqSource,
    SenseSource,
    KafkaSource,
    CsvSource,
    BinarySource,
    DefaultSource,
    LogicalStreamSource
};

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SourceDescriptor : public std::enable_shared_from_this<SourceDescriptor> {

  public:
    SourceDescriptor(SchemaPtr schema) : schema(schema){};

    /**
     * @brief Returns the source descriptor type of this source descriptor.
     * @return
     */
    virtual SourceDescriptorType getType() = 0;

    /**
     * @brief Returns the schema, which is produced by this source descriptor
     * @return SchemaPtr
     */
    const SchemaPtr getSchema() const {
        return schema;
    }

    /**
    * @brief Checks if the source descriptor is of type SourceType
    * @tparam SourceType
    * @return bool true if source descriptor is of SourceType
    */
    template<class SourceType>
    const bool instanceOf() {
        if (dynamic_cast<SourceType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the source descriptor to a SourceType
    * @tparam SourceType
    * @return returns a shared pointer of the SourceType
    */
    template<class SourceType>
    std::shared_ptr<SourceType> as() {
        if (instanceOf<SourceType>()) {
            return std::dynamic_pointer_cast<SourceType>(this->shared_from_this());
        } else {
            NES_FATAL_ERROR("SourceDescriptor: We performed an invalid cast");
            throw std::bad_cast();
        }
    }

  private:
    SchemaPtr schema;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
