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
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Util/Common.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/// This class represents the physical stream join build operator and gets translated to a join build operator
class PhysicalStreamJoinBuildOperator : public PhysicalUnaryOperator, public AbstractEmitOperator
{
public:
    PhysicalStreamJoinBuildOperator(
        const SchemaPtr& inputSchema,
        const SchemaPtr& outputSchema,
        const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& operatorHandler,
        QueryCompilation::StreamJoinStrategy joinStrategy,
        TimestampField timestampField,
        JoinBuildSideType buildSide,
        OperatorId id = getNextOperatorId());

    /// Performs a deep copy of this physical operator
    OperatorPtr copy() override;

    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& getJoinOperatorHandler() const;
    StreamJoinStrategy getJoinStrategy() const;
    const TimestampField& getTimeStampField() const;
    JoinBuildSideType getBuildSide() const;

private:
    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler> streamJoinOperatorHandler;
    const QueryCompilation::StreamJoinStrategy joinStrategy;
    const TimestampField timeStampField;
    const JoinBuildSideType buildSide;
};
}
