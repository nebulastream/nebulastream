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

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES
{

/// Representation of SourceDescriptor in query plan.
class LogicalSourceDescriptor : public SourceDescriptor
{
public:
    static std::unique_ptr<SourceDescriptor> create(std::string logicalSourceName, std::string sourceName);

    [[nodiscard]] bool equal(SourceDescriptor& other) const override;

    std::string toString() const override;

private:
    explicit LogicalSourceDescriptor(std::string logicalSourceName, std::string sourceName);
};

using LogicalSourceDescriptorPtr = std::shared_ptr<LogicalSourceDescriptor>;

} /// namespace NES
