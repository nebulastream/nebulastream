#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <Util/Logger.hpp>
#include <iostream>
#include <memory>

namespace NES {

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class SourceDescriptor : public std::enable_shared_from_this<SourceDescriptor> {

  public:
    /**
     * @brief Creates a new source descriptor without a streamName.
     * @param schema the source schema
     */
    explicit SourceDescriptor(SchemaPtr schema);

    /**
    * @brief Creates a new source descriptor with a streamName.
    * @param schema the source schema
    */
    SourceDescriptor(SchemaPtr schema, std::string streamName);

    /**
     * @brief Returns the schema, which is produced by this source descriptor
     * @return SchemaPtr
     */
    SchemaPtr getSchema();

    /**
    * @brief Checks if the source descriptor is of type SourceType
    * @tparam SourceType
    * @return bool true if source descriptor is of SourceType
    */
    template<class SourceType>
    bool instanceOf() {
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

    /**
     * @brief Indicates if the source descriptor has a stream name.
     * @return returns true if the stream name is defined.
     */
    bool hasStreamName();

    /**
     * @brief Returns the streamName. If no streamName is defined it returns the empty string.
     * @return streamName
     */
    std::string getStreamName();

    /**
     * @brief Returns the string representation of the source descriptor.
     * @return string
     */
    virtual std::string toString() = 0;

    /**
     * @brief Checks if two source descriptors are the same.
     * @param other source descriptor.
     * @return true if both are the same.
     */
    virtual bool equal(SourceDescriptorPtr other) = 0;

    /**
     * @brief Destructor
     */
    virtual ~SourceDescriptor() = default;

  private:
    SchemaPtr schema;
    std::string streamName;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
