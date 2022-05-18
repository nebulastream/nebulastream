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

#ifndef NES_QUERY3_HPP
#define NES_QUERY3_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * AdHoc Query 3
 * SELECT (SUM (total_cost_this_week)) /
 * (SUM (total_duration_this_week)) as cost ratio FROM AnalyticsMatrix
 * GROUP BY number_of_calls_this_week
 * LIMIT 100;
 */
class AdHocQuery3 : public Runtime::Execution::ExecutablePipelineStage {
public:
    //AdHocQuery3(std::shared_ptr<MV> mv) : mv(mv) {};

    struct ResultRecord {
        float result;
    };

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<ResultRecord>();

        int32_t beta = 10; // TODO: random
        uint32_t limit = 100;

        /*int32_t[] weeklyCalls = entry.second.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET];
        int64_t[] weeklyCostSums = entry.second.longAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + COST_SUM];
        int32_t[] weeklyDurationSums = entry.second.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM];*/

        /*  grouped_state;*/

        float sum = 0;
        //for (auto entry : mv->begin()) {
            // TODO: Calc
        //}
        record->result = sum;

        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }

private:
    //std::shared_ptr<MV> mv;
};

#endif //NES_QUERY3_HPP
