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

#pragma once

#include <memory>
#include <Exceptions/RuntimeException.hpp>
namespace NES
{

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SourceDescriptor : public std::enable_shared_from_this<SourceDescriptor>
{
public:
    /**
     * @brief Creates a new source descriptor with a logicalSourceName.
     * @param schema the source schema
     */
    explicit SourceDescriptor(SchemaPtr schema);

    SourceDescriptor(SchemaPtr schema, std::string logicalSourceName);

    /**
     * @brief Returns the schema, which is produced by this source descriptor
     * @return SchemaPtr
     */
    SchemaPtr getSchema() const;

    /**
    * @brief Checks if the source descriptor is of type SourceType
    * @tparam SourceType
    * @return bool true if source descriptor is of SourceType
    */
    template <class SourceType>
    bool instanceOf() const
    {
        if (dynamic_cast<const SourceType*>(this))
        {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the source descriptor to a SourceType
    * @tparam SourceType
    * @return returns a shared pointer of the SourceType
    */
    template <class SourceType>
    std::shared_ptr<SourceType> as() const
    {
        if (instanceOf<SourceType>())
        {
            return std::dynamic_pointer_cast<SourceType>(this->shared_from_this());
        }
        throw Exceptions::RuntimeException("SourceDescriptor: We performed an invalid cast");
    }
    template <class SourceType>
    std::shared_ptr<SourceType> as()
    {
        return std::const_pointer_cast<SourceType>(const_cast<const SourceDescriptor*>(this)->as<const SourceType>());
    }

    template <class SourceType>
    std::shared_ptr<SourceType> as_if()
    {
        return std::dynamic_pointer_cast<SourceType>(this->shared_from_this());
    }

    /**
     * @brief Returns the logicalSourceName. If no logicalSourceName is defined it returns the empty string.
     * @return logicalSourceName
     */
    std::string getLogicalSourceName() const;

    /**
     * @brief Set schema of the source
     * @param schema the schema
     */
    void setSchema(const SchemaPtr& schema);

    /**
     * @brief Returns the string representation of the source descriptor.
     * @return string
     */
    virtual std::string toString() const = 0;

    /**
     * @brief Checks if two source descriptors are the same.
     * @param other source descriptor.
     * @return true if both are the same.
     */
    [[nodiscard]] virtual bool equal(SourceDescriptorPtr const& other) const = 0;

    virtual SourceDescriptorPtr copy() = 0;

    /**
     * @brief Destructor
     */
    virtual ~SourceDescriptor() = default;

protected:
    SchemaPtr schema;
    std::string logicalSourceName;
};

} /// namespace NES
