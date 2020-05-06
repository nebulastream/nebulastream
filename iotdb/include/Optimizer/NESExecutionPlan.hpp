#ifndef NESEXECUTIONPLAN_HPP
#define NESEXECUTIONPLAN_HPP

#include <Optimizer/ExecutionGraph.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>

namespace NES {

using namespace web;

class NESExecutionPlan {

  public:
    NESExecutionPlan();

    void freeResources();

    ExecutionNodePtr getRootNode() const;

    /**
   * @brief Create execution node containing information about the chain of operators to be executed
   * @param operatorName: Name of the operator to be executed (contains "=>" separated actual operator names with their ids)
   * @param nodeName : name of the node (usually a unique identifier)
   * @param nesNode : information about actual nes node where the operators will be executed
   * @param executableOperator : chain of operators to be executed
   * @return pointer to the execution node
   */
    ExecutionNodePtr createExecutionNode(std::string operatorName,
                                         std::string nodeName,
                                         NESTopologyEntryPtr nesNode,
                                         OperatorPtr executableOperator);

    /**
   * @brief Create execution node link between sourceNode and destinationNode nodes
   * @param id : link id
   * @param sourceNode : node which generate the data
   * @param destinationNode : node which consumes the data
   * @param linkCapacity : capacity of the link
   * @param linkLatency : latency of the link
   * @return return execution node link pointer
   */
    ExecutionNodeLinkPtr createExecutionNodeLink(size_t id,
                                                 ExecutionNodePtr sourceNode,
                                                 ExecutionNodePtr destinationNode,
                                                 size_t linkCapacity,
                                                 size_t linkLatency);

    /**
   * @brief method to return the topology plan as a string
   * @return plan as a string
   */
    std::string getTopologyPlanString() const;

    /**
   * @brief method to get the Execution graph
   * @return execution graph
   */
    ExecutionGraphPtr getExecutionGraph() const;

    /**
   * @brief method to check if the plan has a particular vertex
   * @param id of the vertex
   * @return bool indicating existence
   */
    bool hasVertex(int searchId);

    /**
   * @brief method to get a execution ode
   * @param id of the node
   * @return ptr to the execution node
   */
    ExecutionNodePtr getExecutionNode(int searchId);

    /**
   * @brief method to get the execution graph as a json object
   * @return json containing the graph
   */
    json::value getExecutionGraphAsJson() const;

    /**
   * @brief getter of the compute time
   * @return compilation time of this plan
   */
    long getTotalComputeTimeInMillis() const;

    /**
   * @brief setter of the compute time
   * @param compilation time of this plan
   */
    void setTotalComputeTimeInMillis(long totalComputeTime);

  private:
    ExecutionGraphPtr exeGraphPtr;
    long totalComputeTimeInMillis;

    /**
   * @brief method to get all children of a node as json
   * @param ptr to the parrent node
   * @return json value of all children
   */
    vector<json::value> getChildrenNode(ExecutionNodePtr nesParentNode) const;
};

typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

}// namespace NES

#endif//NESEXECUTIONPLAN_HPP
