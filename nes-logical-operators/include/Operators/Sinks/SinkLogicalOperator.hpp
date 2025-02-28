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
#include "Operators/LogicalOperator.hpp"
#include "Operators/UnaryLogicalOperator.hpp"
#include "Sinks/SinkDescriptor.hpp"

namespace NES
{

class SinkLogicalOperator final : public UnaryLogicalOperator
{
public:
    /// During deserialization, we don't need to know/use the name of the sink anymore.
    SinkLogicalOperator() : Operator(), UnaryLogicalOperator() {};

    /// During query parsing, we require the name of the sink and need to assign it an id.
    SinkLogicalOperator(std::string sinkName)
        : Operator(), UnaryLogicalOperator(), sinkName(std::move(sinkName)) {};

    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    bool inferSchema() override;

    const Sinks::SinkDescriptor& getSinkDescriptorRef() const;

    std::unique_ptr<Operator> clone() const override;

    std::string sinkName;
    std::unique_ptr<Sinks::SinkDescriptor> sinkDescriptor;

    [[nodiscard]] SerializableOperator serialize() const override;

    std::string toString() const override;
};
}
