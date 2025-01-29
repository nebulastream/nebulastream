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
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <Util/Execution.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/// This class represents the physical stream join probe operator and gets translated to a join probe operator
class PhysicalStreamJoinProbeOperator : public PhysicalBinaryOperator, public AbstractScanOperator
{
public:
    PhysicalStreamJoinProbeOperator(
        const std::shared_ptr<Schema>& leftSchema,
        const std::shared_ptr<Schema>& rightSchema,
        const std::shared_ptr<Schema>& outputSchema,
        const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& operatorHandler,
        Configurations::StreamJoinStrategy joinStrategy,
        std::unique_ptr<Runtime::Execution::Functions::Function> joinFunction,
        const std::vector<std::string>& joinFieldNamesLeft,
        const std::vector<std::string>& joinFieldNamesRight,
        WindowMetaData windowMetaData,
        OperatorId id = getNextOperatorId());

    /// Performs a deep copy of this physical operator
    std::shared_ptr<Operator> copy() override;

    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& getJoinOperatorHandler() const;
    Configurations::StreamJoinStrategy getJoinStrategy() const;
    std::unique_ptr<Runtime::Execution::Functions::Function> getJoinFunction();
    std::vector<std::string> getJoinFieldNameLeft() const;
    std::vector<std::string> getJoinFieldNameRight() const;
    Runtime::Execution::JoinSchema getJoinSchema() const;
    const WindowMetaData& getWindowMetaData() const;

protected:
    std::string toString() const override;
    std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler> streamJoinOperatorHandler;
    Configurations::StreamJoinStrategy joinStrategy;
    std::unique_ptr<Runtime::Execution::Functions::Function> joinFunction;
    std::vector<std::string> joinFieldNamesLeft;
    std::vector<std::string> joinFieldNamesRight;
    WindowMetaData windowMetaData;
};
}
