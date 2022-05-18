/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_QUERY7_HPP
#define NES_QUERY7_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * Adhoc Query 7
 * SELECT (SUM (total_cost_this_week)) /
 * (SUM (total_duration_this_week))
 * FROM AnalyticsMatrix
 * WHERE CellValueType = v;
 */

class AdHocQuery7 : public Runtime::Execution::ExecutablePipelineStage {
public:
    //AdHocQuery7(std::shared_ptr<MV> mv) : mv(mv) {};

    struct ResultRecord {
        float result;
    };

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<ResultRecord>();

        // TODO:

        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }

private:
    //std::shared_ptr<MV> mv;
};

#endif //NES_QUERY7_HPP
