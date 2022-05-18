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

#ifndef NES_QUERY1_HPP
#define NES_QUERY1_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Views/TransactionManager.hpp>

/**
 * AdHoc Query 1
 * SELECT AVG(total_duration_week)
 * FROM Matrix
 * WHERE total_number_calls_week > beta
 */
template <typename MV>
class AdHocQuery1 : public Runtime::Execution::ExecutablePipelineStage {
public:
    AdHocQuery1(std::shared_ptr<MV> mv) : mv(mv) {};

    struct ResultRecord {
        float result;
    };

    /**
     * Query calculation Ad hoc query 1
     *
     * AdHoc Query 1
     * SELECT AVG(total_duration_week)
     * FROM Matrix
     * WHERE total_number_calls_week > beta
     *
     * @param buffer
     * @param ctx
     * @param wc
     * @return
     */
    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<ResultRecord>();

        int32_t beta = 0; // TODO: random
        uint64_t count = 0;
        float total = 0;

        for (auto it = mv->begin(); it != mv->end(); ++it) {
            if ((*it).value.intAggs[LOCAL_INT_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET] > beta){
                total += (*it).value.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM_OFFSET];
                ++count;
            }
        }

        record->result = total / count;
        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }
private:
    std::shared_ptr<MV> mv;
};

/**
 * Query calculation used for Ad Hoc centered computation
 *
 * alculate on AMRecord entries
 *
 * @param buffer
 * @param ctx
 * @param wc
 * @return
 */
/*ExecutionResult execute(Runtime::TupleBuffer& buffer,
                        Runtime::Execution::PipelineExecutionContext& ctx,
                        Runtime::WorkerContext& wc) override {
    auto record = buffer.getBuffer<ResultRecord>();

    int32_t beta = 10; // TODO: random
    float count = 0;
    float total = 0;
    for (auto it = mv->begin(), end = mv->end(); it != end; ++it) {
        if ((* it).value.intAggs[LOCAL_INT_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET] > beta){
                count++;
                total += (* it).value.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM_OFFSET];
        }
    }
    record->result = total / count;
    ctx.emitBuffer(buffer, wc);
    return ExecutionResult::Ok;
}*/
/*ExecutionResult execute(Runtime::TupleBuffer& buffer,
                        Runtime::Execution::PipelineExecutionContext& ctx,
                        Runtime::WorkerContext& wc) override {
    auto record = buffer.getBuffer<ResultRecord>();

    int32_t beta = 0; // TODO: random
    uint64_t count = 0;
    float total = 0;

    // Combine using HT
    //cuckoohash_map<uint64_t, std::shared_ptr<AMRecord>> map;
    std::multimap<uint64_t, std::shared_ptr<AMRecord>> map;

    std::shared_ptr<MaterializedView<uint64_t, EventRecord>> mv_;
    if (auto mv_ = dynamic_pointer_cast<MaterializedViewTableBasedAdHocCentered<uint64_t>>(mv)){
        for (auto it = mv_->begin(), end = mv_->end(); it != end; ++it) {
            if (map.contains((* it).first)) {
                map.find((* it).first)->second->update((* it).second);
            } else {
                auto val = std::make_shared<AMRecord>((* it).second);
                map.insert({(* it).first, val});
            }
        }
    } else if (auto mv_ = dynamic_pointer_cast<LogBasedMV<uint64_t>>(mv)){
        for (auto it = mv_->begin(); it != mv_->end(); it++) {
            // Check for the version
            if ((*it).value.intAggs[LOCAL_INT_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET] > beta){
                total += (*it).value.intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM_OFFSET];
                count++;
            }
        }
    } else if (auto mv_ = dynamic_pointer_cast<Log<uint64_t>>(mv)){
        for (auto it = mv_->begin(), end = mv_->end(); it != end; ++it) {
            if (map.contains((* it).key)) {
                map.find((* it).key)->second->update((* it).value);
            } else {
                auto val = std::make_shared<AMRecord>((* it).value);
                map.insert({(* it).key, val});
            }
        }
    } else {
        NES_ERROR("Type not supported");
    }

    std::vector<int64_t> vec = {};
    // Iterate through HT
    for (auto entry : map) {
        if (entry.second->intAggs[LOCAL_INT_OFFSET + WEEKLY_OFFSET + CALLS_COUNT_OFFSET] > beta){
            count++;
            total += entry.second->intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM_OFFSET];
            vec.push_back(entry.second->intAggs[NO_FILTER_OFFSET + WEEKLY_OFFSET + DURATION_SUM_OFFSET]);
        }
    }

    record->result = total / count;
    NES_INFO("record->result: " << total << " " << record->result);
    // 56660 236.083
    //for (auto i : vec)
    //    std::cout << i << " ";
    //std::cout << "\n";
    ctx.emitBuffer(buffer, wc);
    return ExecutionResult::Ok;
}*/

#endif //NES_QUERY1_HPP
