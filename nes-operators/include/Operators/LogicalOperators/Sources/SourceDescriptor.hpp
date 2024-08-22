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
    explicit SourceDescriptor(SchemaPtr schema);

    explicit SourceDescriptor(SchemaPtr schema, std::string logicalSourceName);

    explicit SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string sourceName);

    SchemaPtr getSchema() const;

    template <class SourceType>
    bool instanceOf() const
    {
        if (dynamic_cast<const SourceType*>(this))
        {
            return true;
        }
        return false;
    };

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

    std::string getLogicalSourceName() const;

    void setSchema(const SchemaPtr& schema);

    virtual std::string toString() const = 0;

    [[nodiscard]] virtual bool equal(SourceDescriptorPtr const& other) const = 0;

    virtual SourceDescriptorPtr copy() = 0;

    virtual ~SourceDescriptor() = default;

    [[nodiscard]] std::string getSourceName() const;

    void setSourceName(std::string sourceName);

private:
    SchemaPtr schema;
    std::string logicalSourceName;
    std::string sourceName;
};

} /// namespace NES
