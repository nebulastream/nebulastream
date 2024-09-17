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

#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Abstract class that represents a physical stream join build or probe operator
 */
class PhysicalStreamJoinOperator
{
public:
    PhysicalStreamJoinOperator(
        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& joinOperatorHandler,
        QueryCompilation::StreamJoinStrategy joinStrategy);

    StreamJoinStrategy getJoinStrategy() const;

    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& getJoinOperatorHandler() const;

protected:
    Runtime::Execution::Operators::StreamJoinOperator streamJoinOperator;
    Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr joinOperatorHandler;
};
}
