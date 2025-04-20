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

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <RunningQueryPlan.hpp>
#include <Task.hpp>

namespace NES
{
StopPipelineTask::~StopPipelineTask() = default;

StopPipelineTask::StopPipelineTask(StopPipelineTask&& other) noexcept = default;
StopPipelineTask& StopPipelineTask::operator=(StopPipelineTask&& other) noexcept = default;

StopPipelineTask::StopPipelineTask(
    QueryId queryId, std::unique_ptr<RunningQueryPlanNode> pipeline, onComplete complete, onFailure failure) noexcept
    : BaseTask(queryId, std::move(complete), std::move(failure)), pipeline(std::move(pipeline))
{
}

}
