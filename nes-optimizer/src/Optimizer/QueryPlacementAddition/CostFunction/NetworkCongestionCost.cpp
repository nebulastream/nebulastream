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
#include <Optimizer/QueryPlacementAddition/CostFunction/NetworkCongestionCost.hpp>

NES::Optimizer::NetworkCongestionCost::NetworkCongestionCost(uint32_t samplingInterval) : samplingInterval(samplingInterval) {}

float NES::Optimizer::NetworkCongestionCost::getCost(PlacementMatrix candidate) {
    float totalNetworkCongestion = 0;

    // iterate over all nodes
    for (uint32_t rowIdx = 0; rowIdx < candidate.size(); rowIdx++) {
        auto currentWorkerId = stubGetExecutionNodeFromCandidateEnetry(candidate, rowIdx);

        // sum the network congestion cost from each node
        totalNetworkCongestion += std::max((float) 0, (getNodeOutputRate(candidate, rowIdx) - stubGetNodeOutputBandwidth(currentWorkerId)));
    }

    return totalNetworkCongestion;
}

NES::OperatorId NES::Optimizer::NetworkCongestionCost::stubGetOperatorIdFromCandidateEntry([[maybe_unused]] const NES::PlacementMatrix& candidate,
                                                                                           [[maybe_unused]] uint32_t row,
                                                                                           [[maybe_unused]] uint32_t col) {
    // TODO #4664: implement actual method to obtain the operator Id from a given global query plan
    return INVALID_OPERATOR_ID;
}
float NES::Optimizer::NetworkCongestionCost::stubGetOperatorSelectivity([[maybe_unused]] uint32_t period,
                                                                        [[maybe_unused]] OperatorId operatorId) {
    // TODO #4664: implement an actual call from the statistic coordinator
    return 0;
}
float NES::Optimizer::NetworkCongestionCost::stubGetNodeIngestionRate([[maybe_unused]] uint32_t period,
                                                                      [[maybe_unused]] NES::WorkerId WorkerId) {
    // TODO #4664: implement an actual call from the statistic coordinator
    return 0;
}
float NES::Optimizer::NetworkCongestionCost::getNodeOutputRate(const PlacementMatrix &candidate, uint32_t rowIdx) {
    auto currentRow = candidate[rowIdx];

    // Set initial selectivity to 1
    float nodeSelectivity = 1;

    // loop over all operators in the node
    for (const auto colIdx: currentRow) {
        auto currentOperatorId = stubGetOperatorIdFromCandidateEntry(candidate, rowIdx, colIdx);
        auto currentOperatorSelectivity = stubGetOperatorSelectivity(samplingInterval, currentOperatorId);

        // compute node selectivity as a product of all operator's selectivity in that node
        nodeSelectivity *= currentOperatorSelectivity;
    }

    // compute node output rate as the product of node selectivity and node ingestion rate
    auto currentWorkerId = stubGetExecutionNodeFromCandidateEnetry(candidate, rowIdx);
    auto nodeOutputRate = nodeSelectivity * stubGetNodeIngestionRate(samplingInterval, currentWorkerId);

    return nodeOutputRate;
}

float NES::Optimizer::NetworkCongestionCost::stubGetNodeOutputBandwidth([[maybe_unused]] NES::WorkerId WorkerId) {
    // TODO #4664: implement an actual call from the statistic coordinator
    return 0;
}
NES::WorkerId
NES::Optimizer::NetworkCongestionCost::stubGetExecutionNodeFromCandidateEnetry([[maybe_unused]] const NES::PlacementMatrix& candidate,
                                                                               [[maybe_unused]] uint32_t rowIdx) {
    // TODO #4664: implement actual method to obtain the execution node Id from a given global query plan

    return INVALID_WORKER_NODE_ID;
}
