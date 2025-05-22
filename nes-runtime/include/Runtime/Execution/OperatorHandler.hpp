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
#include <cstdint>
#include <Runtime/QueryTerminationType.hpp>

namespace NES::Runtime::Execution
{
/// Forward declaration of PipelineExecutionContext, which directly includes OperatorHandler
class PipelineExecutionContext;

/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler
{
public:
    OperatorHandler() = default;

    virtual ~OperatorHandler() = default;

    virtual void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) = 0;

    virtual void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) = 0;
};

}
