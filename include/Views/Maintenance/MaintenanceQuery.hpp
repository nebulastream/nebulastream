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

#ifndef NES_MAINTENANCEQUERY_HPP
#define NES_MAINTENANCEQUERY_HPP

#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * 1) On new stream tuple:
 *  δx ← Maintenance-Query(Input-Tuples)
 *  x ← MV.get()
 *  MV.write(Window-Aggregate(δx, x))
 * 2) On ad how query:
 *  result ← Ad-Hoc-Query(MV.scan())
 * 3) On window end:
 *  MV.clear()
 */
template <typename MV>
class MaintenanceQuery : public Runtime::Execution::ExecutablePipelineStage {
public:
    MaintenanceQuery(std::shared_ptr<MV> mv) : mv(mv) {};

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        EventRecord *record = buffer.getBuffer<EventRecord>();

        for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
                if (record[i].mv$timestamp == 0) {
                    NES_INFO("New window!");
                    //mv->newWindow();
                }
                mv->insert(record[i].mv$id, record[i]);
        }
        ctx.emitBuffer(buffer, wc);
        return ExecutionResult::Ok;
    }
private:
    std::shared_ptr<MV> mv;
};

#endif //NES_MAINTENANCEQUERY_HPP
