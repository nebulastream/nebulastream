/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef IOTDB_NESPLACEMENTOPTIMIZER_HPP
#define IOTDB_NESPLACEMENTOPTIMIZER_HPP

#include <iostream>
#include "../Topology/NESTopologyManager.hpp"
#include "../Topology/NESTopologyPlan.hpp"
#include "NESExecutionPlan.hpp"

namespace iotdb {
class NESPlacementOptimizer {
 private:

 public:
  NESPlacementOptimizer() {};

  /**
   * @brief Returns an execution graph based on the input query and nes topology.
   * @param inputQuery
   * @param nesTopologyPlan
   * @return
   */
  virtual NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) = 0;

  void invalidateUnscheduledOperators(OperatorPtr &rootOperator, vector<int> &childOperatorIds);

  /**
   * @brief This method will traverse through all the nodes of the graphs and remove any reference to the operator not 
   * located on the traversed node.
   * 
   * @param graph 
   */
  void removeNonResidentOperators(NESExecutionPlan graph);

  /**
   * @brief This method will add system generated zmq source and sinks for each execution node.
   * @note We use zmq for internal message transfer therefore the source and sink will be zmq based.
   * @note This method will not append zmq source or sink if the operator chain in an execution node already contains
   * user defined source or sink operator respectively.
   *
   * @param schema
   * @param graph
   */
  void addSystemGeneratedSourceSinkOperators(const Schema &schema, NESExecutionPlan graph);

  /**
   * @brief Fill the execution graph with forward operators in nes topology.
   * @param graph 
   * @param nesTopologyPtr
   */
  void completeExecutionGraphWithNESTopology(NESExecutionPlan graph, NESTopologyPlanPtr nesTopologyPtr);

  /**
   * @brief Factory method returning different kind of optimizer.
   * @param optimizerName
   * @return instance of type BaseOptimizer
   */
  static std::shared_ptr<NESPlacementOptimizer> getOptimizer(std::string optimizerName);

  /**
   * @brief Get all candidate node from sink to the target source node.
   * @param nesGraphPtr
   * @param targetSource
   * @return deque containing nes nodes with top element being sink node and bottom most being the targetSource node.
   */
  deque<NESTopologyEntryPtr> getCandidateNESNodes(const NESGraphPtr &nesGraphPtr,
                                                  const NESTopologyEntryPtr &targetSource) const;
  void convertFwdOptr(const Schema &schema, ExecutionNodePtr &executionNodePtr) const;
};

}

#endif //IOTDB_NESPLACEMENTOPTIMIZER_HPP
