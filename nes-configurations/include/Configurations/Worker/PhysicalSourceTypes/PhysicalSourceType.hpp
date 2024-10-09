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
#include <string>

namespace NES
{

enum class SourceType : uint8_t
{
    CSV_SOURCE,
    TCP_SOURCE,
};

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

/**
 * @brief Interface for different Physical Source types
 */
class PhysicalSourceType : public std::enable_shared_from_this<PhysicalSourceType>
{
public:
    PhysicalSourceType(std::string logicalSourceName, SourceType sourceType);

    virtual ~PhysicalSourceType() noexcept = default;

    /**
     * Checks equality
     * @param other sourceType to check equality for
     * @return true if equal, false otherwise
     */
    virtual bool equal(PhysicalSourceTypePtr const& other) = 0;

    /**
     * @brief creates a string representation of the source
     * @return
     */
    virtual std::string toString() = 0;

    /**
     * @brief returns the logical source name this physical source contributes to
     * @return the logical source name
     */
    const std::string& getLogicalSourceName() const;

    /**
     * @brief Return source type
     * @return enum representing source type
     */
    SourceType getSourceType();

    /**
     * @brief Return source type
     * @return string representing source type
     */
    std::string getSourceTypeAsString();

    /**
     * @brief reset the values to default
     */
    virtual void reset() = 0;

private:
    std::string logicalSourceName;
    SourceType sourceType;
};

} /// namespace NES
