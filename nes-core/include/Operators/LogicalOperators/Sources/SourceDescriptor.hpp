/*
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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_

#include <Operators/OperatorId.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>
namespace NES {

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceDescriptor : public std::enable_shared_from_this<SourceDescriptor> {

  public:
    enum InputFormat { JSON, CSV };

    /**
     * @brief Creates a new source descriptor with a logicalSourceName.
     * @param schema the source schema
     */
    SourceDescriptor(SchemaPtr schema);

    /**
     * @brief Creates a new source descriptor with a logicalSourceName.
     * @param schema the source schema
     * @param logicalSourceName the logical source name
     */
    SourceDescriptor(SchemaPtr schema, std::string logicalSourceName);

    /**
     * @brief Creates a new source descriptor with a logicalSourceName.
     * @param schema the source schema
     * @param logicalSourceName the logical source name
     * @param physicalSourceName the physical source name
     */
    SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string physicalSourceName);

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
        }
        NES_FATAL_ERROR("SourceDescriptor: We performed an invalid cast");
        throw std::bad_cast();
    }

    /**
     * @brief Returns the logicalSourceName. If no logicalSourceName is defined it returns the empty string.
     * @return logicalSourceName
     */
    std::string getLogicalSourceName();

    /**
     * @brief Returns the logicalSourceName. If no logicalSourceName is defined it returns the empty string.
     * @return logicalSourceName
     */
    std::string getPhysicalSourceName();

    /**
     * @brief Set physical source name
     * @param physicalSourceName : name of the physical source
     */
    void setPhysicalSourceName(const std::string& physicalSourceName);

    /**
     * @brief Set schema of the source
     * @param schema the schema
     */
    void setSchema(const SchemaPtr& schema);

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
    [[nodiscard]] virtual bool equal(SourceDescriptorPtr const& other) = 0;

    virtual SourceDescriptorPtr copy() = 0;

    /**
     * @brief Destructor
     */
    virtual ~SourceDescriptor() = default;

  protected:
    SchemaPtr schema;
    std::string logicalSourceName;
    std::string physicalSourceName;
};

}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_SOURCEDESCRIPTOR_HPP_
