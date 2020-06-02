#ifndef NES_GLOBALEXECUTIONPLAN_HPP
#define NES_GLOBALEXECUTIONPLAN_HPP

#include <list>
#include <map>
#include <memory>

namespace NES {

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

/**
 * This class holds the global execution plan for the NES system. The nodes in this graph are represented by ExecutionNode class.
 */
class GlobalExecutionPlan {

    static GlobalExecutionPlanPtr create();
    ~GlobalExecutionPlan() = default;

    /**
     * Remove the execution node from the graph
     * @param id: id of the execution node to be removed
     * @return true if operation succeeds
     */
    //TODO: what should we do about its children? Also, a good location to release the occupied resources.
    bool removeExecutionNode(uint64_t id);

    /**
     * Find is execution node exists.
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    bool executionNodeExists(uint64_t id);

    /**
     * Get the execution node
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    ExecutionNodePtr getExecutionNode(uint64_t id);

    /**
     * Add execution node as child of another execution node
     * @param parentId: id of the parent execution node
     * @param childExecutionNode: the child execution node
     * @return true if operation succeeds
     */
    bool addExecutionNodeAsChildTo(uint64_t parentId, ExecutionNodePtr childExecutionNode);

    /**
     * Add execution node as parent of another execution node
     * @param childId: id of the child node
     * @param parentExecutionNode: the parent execution node
     * @return true if operation succeeds
     */
    bool addExecutionNodeAsParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode);

  private:
    GlobalExecutionPlan() = default;

    /**
     * Index based on nodeId for faster access to the nodes
     */
    std::map<uint64_t, ExecutionNodePtr> nodeIdIndex;
};

}// namespace NES

#endif//NES_GLOBALEXECUTIONPLAN_HPP
