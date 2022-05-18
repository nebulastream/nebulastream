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

#ifndef NES_QUERY2_HPP
#define NES_QUERY2_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * AdHoc Query 2
 * SELECT MAX (most_expensive_call_this_week)
 * FROM AnalyticsMatrix
 * WHERE total_number_of_calls_this_week > β;
 */
class AdHocQuery2 : public Runtime::Execution::ExecutablePipelineStage {
public:

    struct ResultRecord {
        float result;
    };

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<ResultRecord>();

        int32_t beta = 10; // TODO: random
        float max = 0;
        //for (auto it = mv->begin(), end = mv->end(); it != end; ++it) {
            /*if (entry.second.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET] > beta){
                max = std::max(entry.second.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + COST_MAX_OFFSET], max);
            }*/
        //}
        record->result = max;

        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }

private:
    //std::shared_ptr<MV> mv;
};

#endif //NES_QUERY2_HPP