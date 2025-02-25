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
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

class SinkLogicalOperator : public LogicalUnaryOperator
{
public:
    /// During deserialization, we don't need to know/use the name of the sink anymore.
    SinkLogicalOperator(OperatorId id) : Operator(id), LogicalUnaryOperator(id) {};

    /// During query parsing, we require the name of the sink and need to assign it an id.
    SinkLogicalOperator(std::string sinkName, const OperatorId id)
        : Operator(id), LogicalUnaryOperator(id), sinkName(std::move(sinkName)) {};

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    bool inferSchema() override;

    const Sinks::SinkDescriptor& getSinkDescriptorRef() const;
    std::shared_ptr<Sinks::SinkDescriptor> getSinkDescriptor() const;

    std::shared_ptr<Operator> copy() override;
    void inferStringSignature() override;

    std::string sinkName;
    std::shared_ptr<Sinks::SinkDescriptor> sinkDescriptor;

protected:
    std::string toString() const override;
};
}
