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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_NETWORKCONGESTIONCOST_HPP
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_NETWORKCONGESTIONCOST_HPP

#include <Identifiers.hpp>
#include <Optimizer/QueryPlacementAddition/CostFunction/AbstractQueryPlacementCost.hpp>

namespace NES::Optimizer {
/**
 * Represent a network congestion (i.e., bandwidth over-utilization) as a result of a placement.
 */
class NetworkCongestionCost : public AbstractQueryPlacementCost {
  public:
    NetworkCongestionCost(uint32_t samplingInterval);

    float getCost(PlacementMatrix candidate) override;

  private:
    /**
     * @brief calculate the total output rate of event streams produced by all running pipelines on a single node.
     * @param candidate the placement matrix of the candidate
     * @param rowIdx the row representing the node to compute
     * @return bytes of event stream produced per second.
     */
    float getNodeOutputRate(const PlacementMatrix &candidate, uint32_t rowIdx);

    /**
     * @brief obtain the average ingestion rate in the last 'period' of second from the Statistic coordinator
     * @param period time in seconds
     * @param executionNodeId id of the node to query
     * @return ingestion rate in tuple/s
     * TODO #4664: implement an actual call from the statistic coordinator
     */
    float stubGetNodeIngestionRate(uint32_t period, ExecutionNodeId executionNodeId);

    /**
     * @brief obtain the selectivity for the given 'pinnedUpStreamOperator'
     * @param period time in seconds
     * @param pinnedUpstreamOperators operator of which the selectivity is to be identified
     * @return a fraction representing operator's selectivity
     * TODO #4664: implement an actual call from the statistic coordinator
     */
    float stubGetOperatorSelectivity(uint32_t period, OperatorId operatorId);

    /**
     * @brief obtain the output bandwidth of a given 'executionNodeId'
     * @param executionNodeId the id of the node to query
     * @return the output bandwidth of the node in bytes/s
     * TODO #4664: implement an actual call from the statistic coordinator
     */
    float stubGetNodeOutputBandwidth(ExecutionNodeId executionNodeId);

    /**
     * @brief get the actual operatorId from the placement matrix
     * @param candidate placement matrix representing a candidate
     * @param row row in the matrix
     * @param col column in the matrix
     * @return an actual operatorId
     * // TODO #4664: implement actual method to obtain the operator Id from a given global query plan
     */
     OperatorId stubGetOperatorIdFromCandidateEntry(const PlacementMatrix &candidate,
                                                   uint32_t row,
                                                   uint32_t col);

     ExecutionNodeId stubGetExecutionNodeFromCandidateEnetry(const PlacementMatrix &candidate,
                                                             uint32_t rowIdx);

     uint32_t samplingInterval;
};
} // namespace NES::Optimizer
#endif//NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_NETWORKCONGESTIONCOST_HPP
