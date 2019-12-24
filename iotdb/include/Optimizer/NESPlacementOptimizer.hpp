/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef IOTDB_FOGPLACEMENTOPTIMIZER_HPP
#define IOTDB_FOGPLACEMENTOPTIMIZER_HPP

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
   * @brief Returns an execution graph based on the input query and fog topology.
   * @param inputQuery
   * @param fogTopologyPlan
   * @return
   */
  virtual NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr fogTopologyPlan) = 0;

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
   * @brief Fill the execution graph with forward operators in fog topology. 
   * @param graph 
   * @param fogTopologyPtr
   */
  void completeExecutionGraphWithFogTopology(NESExecutionPlan graph, NESTopologyPlanPtr fogTopologyPtr);

  /**
   * @brief Factory method returning different kind of optimizer.
   * @param optimizerName
   * @return instance of type BaseOptimizer
   */
  static std::shared_ptr<NESPlacementOptimizer> getOptimizer(std::string optimizerName);

  /**
   * @brief Get all candidate node from sink to the target source node.
   * @param fogGraphPtr
   * @param targetSource
   * @return deque containing Fog nodes with top element being sink node and bottom most being the targetSource node.
   */
  deque<NESTopologyEntryPtr> getCandidateFogNodes(const NESGraphPtr &fogGraphPtr,
                                                  const NESTopologyEntryPtr &targetSource) const;
  void convertFwdOptr(const Schema &schema, ExecutionNodePtr &executionNodePtr) const;
};

}

#endif //IOTDB_FOGPLACEMENTOPTIMIZER_HPP
