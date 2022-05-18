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

#ifndef NES_QUERY4_HPP
#define NES_QUERY4_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * AdHoc Query 4
 * SELECT city, AVG(number_of_local_calls_this_week),
 *    SUM(total_duration_of_local_calls_this_week) FROM AnalyticsMatrix, RegionInfo
 * WHERE number_of_local_calls_this_week > γ
 * AND total_duration_of_local_calls_this_week > δ
 * AND AnalyticsMatrix.zip = RegionInfo.zip GROUP BY city;
 */

class AdHocQuery4 : public Runtime::Execution::ExecutablePipelineStage {
public:
    //AdHocQuery4(std::shared_ptr<MV> mv) : mv(mv) {};

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

#endif //NES_QUERY4_HPP
