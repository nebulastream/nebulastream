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
#include "TuplePerTaskComputer.hpp"

namespace NES::Runtime {

class LatencyTuplePerTaskComputer : public TuplePerTaskComputer {
public:
    LatencyTuplePerTaskComputer() : increaseTaskSizeFactor(1.1), decreaseTaskSizeFactor(0.9) {}
    ~LatencyTuplePerTaskComputer() override = default;
    void calculateOptimalTaskSize(std::unordered_map<PipelineId, PipelineStatistics>& pipelineStatisticsLocked, std::unordered_map<QueryId, QueryInfo>& queryInfoLocked) const override;

private:
    double increaseTaskSizeFactor;
    double decreaseTaskSizeFactor;
};

}
