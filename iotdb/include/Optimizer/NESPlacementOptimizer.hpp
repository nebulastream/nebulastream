#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <iostream>
#include <Topology/NESTopologyManager.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include "NESExecutionPlan.hpp"

namespace NES {
/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class NESPlacementOptimizer {
 private:

 public:
  NESPlacementOptimizer() {
  }
  ;

  /**
   * @brief Returns an execution graph based on the input query and nes topology.
   * @param inputQuery
   * @param nesTopologyPlan
   * @return
   */
  virtual NESExecutionPlanPtr initializeExecutionPlan(
      InputQueryPtr inputQuery , NESTopologyPlanPtr nesTopologyPlan) = 0;

  /**
   * @brief This method will add system generated zmq source and sinks for each execution node.
   * @note We use zmq for internal message transfer therefore the source and sink will be zmq based.
   * @note This method will not append zmq source or sink if the operator chain in an execution node already contains
   * user defined source or sink operator respectively.
   *
   * @param schema
   * @param nesExecutionPlanPtr
   */
  void addSystemGeneratedSourceSinkOperators(
      const Schema& schema , NESExecutionPlanPtr nesExecutionPlanPtr);

  /**
   * @brief Fill the execution nesExecutionPlanPtr with forward operators in nes topology.
   * @param nesExecutionPlanPtr
   * @param nesTopologyPtr
   */
  void completeExecutionGraphWithNESTopology(
      NESExecutionPlanPtr nesExecutionPlanPtr ,
      NESTopologyPlanPtr nesTopologyPtr);

  /**
   * @brief this methods takes the user specified UDFS from the sample operator and add it to all Sense Operators
   * @param inputQuery
   */
  void setUDFSFromSampleOperatorToSenseSources(
      InputQueryPtr inputQuery);

  /**
   * @brief Factory method returning different kind of optimizer.
   * @param optimizerName
   * @return instance of type BaseOptimizer
   */
  static std::shared_ptr<NESPlacementOptimizer> getOptimizer(
      std::string optimizerName);

  void convertFwdOptr(const Schema& schema ,
                      ExecutionNodePtr executionNodePtr) const;

  /**
   * @brief This method returns the source operator in the user input query
   * @param root: the sink operator of the query
   * @return source operator pointer
   */
  OperatorPtr getSourceOperator(OperatorPtr root);
};

}

#endif //NESPLACEMENTOPTIMIZER_HPP
