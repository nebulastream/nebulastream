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

#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/// This class represents the physical stream join probe operator and gets translated to a join probe operator
class PhysicalStreamJoinProbeOperator : public PhysicalBinaryOperator, public AbstractScanOperator
{
public:
    PhysicalStreamJoinProbeOperator(
        const SchemaPtr& leftSchema,
        const SchemaPtr& rightSchema,
        const SchemaPtr& outputSchema,
        const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& operatorHandler,
        const QueryCompilation::StreamJoinStrategy joinStrategy,
        std::unique_ptr<Runtime::Execution::Functions::Function> joinFunction,
        const std::vector<std::string>& joinFieldNamesLeft,
        const std::vector<std::string>& joinFieldNamesRight,
        const std::string& windowStartFieldName,
        const std::string& windowEndFieldName,
        const OperatorId id = getNextOperatorId());

    /// Performs a deep copy of this physical operator
    OperatorPtr copy() override;

    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& getJoinOperatorHandler() const;
    StreamJoinStrategy getJoinStrategy() const;
    std::unique_ptr<Runtime::Execution::Functions::Function> getJoinFunction();
    std::vector<std::string> getJoinFieldNameLeft() const;
    std::vector<std::string> getJoinFieldNameRight() const;
    const Runtime::Execution::JoinSchema getJoinSchema() const;
    const Runtime::Execution::WindowMetaData& getWindowMetaData() const;

protected:
    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler> streamJoinOperatorHandler;
    const StreamJoinStrategy joinStrategy;
    std::unique_ptr<Runtime::Execution::Functions::Function> joinFunction;
    const std::vector<std::string> joinFieldNamesLeft;
    const std::vector<std::string> joinFieldNamesRight;
    const Runtime::Execution::WindowMetaData windowMetaData;
};
}
