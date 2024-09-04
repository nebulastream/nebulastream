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

namespace NES
{

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

/**
 * @brief This class is used for representing the description of a sink operator
 */
class SinkDescriptor : public std::enable_shared_from_this<SinkDescriptor>
{
public:
    /**
     * @brief Ctor to create a new sink descriptor
     * @param numberOfOrigins: number of origins of a given query
     * @param addTimestamp flat to indicate if timestamp shall be added when writing to sink
     * @return descriptor for sink
     */
    SinkDescriptor(uint64_t numberOfOrigins, bool addTimestamp);
    explicit SinkDescriptor(uint64_t numberOfOrigins);
    SinkDescriptor();

    virtual ~SinkDescriptor() = default;

    /**
     * @brief getter for number of origins
     * @return number of origins
     */
    uint64_t getNumberOfOrigins() const;

    /**
     * @brief getter for addTimestamp field
     * @return addTimestamp
     */
    bool getAddTimestamp() const;

    template <class SinkType>
    std::shared_ptr<SinkType> as_if()
    {
        return std::dynamic_pointer_cast<SinkType>(this->shared_from_this());
    }

    virtual std::string toString() const = 0;
    [[nodiscard]] virtual bool equal(SinkDescriptorPtr const& other) = 0;

protected:
    uint64_t numberOfOrigins;
    bool addTimestamp;
};

} /// namespace NES
