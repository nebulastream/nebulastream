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
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>

namespace NES::QueryCompilation
{
/**
 * @brief Simple phase to add scan and emit operator to pipelines in necessary.
 * A common case would be that, the pipelining phase placed a filter operator in an own pipeline.
 * In this case, the AddScanAndEmitPhase adds a scan before and end emit operator respectively after the filter operator.
 */
class AddScanAndEmitPhase
{
public:
    static std::shared_ptr<AddScanAndEmitPhase> create();
    static std::shared_ptr<PipelineQueryPlan> apply(std::shared_ptr<PipelineQueryPlan> pipeline);
    static std::shared_ptr<OperatorPipeline> process(std::shared_ptr<OperatorPipeline> pipeline);
};
}
