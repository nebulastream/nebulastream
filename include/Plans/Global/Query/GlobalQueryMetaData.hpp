#ifndef NES_GLOBALQUERYMETADATA_HPP
#define NES_GLOBALQUERYMETADATA_HPP

#include <Plans/Query/QueryId.hpp>
#include <Plans/Global/Query/GlobalQueryId.hpp>
#include <memory>
#include <set>
#include <vector>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryNode;
typedef std::shared_ptr<GlobalQueryNode> GlobalQueryNodePtr;

class GlobalQueryMetaData;
typedef std::shared_ptr<GlobalQueryMetaData> GlobalQueryMetaDataPtr;

/**
 * @brief This class holds the meta-data about a collection of QueryPlans.
 * The QueryPlans in the metadata are inter-connected, i.e. from its source nodes we can reach all sink nodes.
 * A Global Query Plan can consists of multiple Global Query Meta Data. However, a query plan con only occur in
 * one of the Global Query Meta Data from the collection within a Global Query Plan.
 *
 * Example:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 * In the above GQP, we have two QueryPlans with GQN1 and GQN5 as respective sink nodes.
 * A GlobalQueryMetaData consists of a unique:
 *  - Global Query Id : this id is equivalent to the Query Id assigned to the user queryId. Since, there can be more than one query that can be merged
 *                      together we generate a unique Global Query Id that can be associated to more than one Query Ids.
 *  - Query Ids : A vector of original Query Ids that shares a common Global Query Id.
 *  - Sink Global Query Nodes : The vector of Global Query Nodes that contains sink operators of all the Query Ids that share a common Global QueryId.
 *  - Deployed : A boolean flag indicating if the query plan is deployed or not.
 *  - NewMetaData : A boolean flag indicating if the meta data is a newly created one (i.e. it was never deployed before).
 *
 *  For the above GQP, following will be the Global Query Metadata:
 *  GQM = {1, {Q1, Q2}, {Sink1, Sink2}, False, True}
 *
 */
class GlobalQueryMetaData {

  public:
    static GlobalQueryMetaDataPtr create(std::set<QueryId> queryIds, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes);

    /**
     * @brief Add a new Set of Global Query Node with sink operators
     * @param globalQueryNodes :  the Global Query Node with sink operators
     */
    void addNewSinkGlobalQueryNodes(const std::set<GlobalQueryNodePtr>& globalQueryNodes);

    /**
     * @brief Remove a Query Id and associated Global Query Node with sink operators
     * @param queryId : the original query Id
     * @return true if successful
     */
    bool removeQueryId(QueryId queryId);

    /**
     * @brief Clear all MetaData information
     */
    void clear();

    /**
     * @brief Get the Query plan for deployment build from the information within this metadata
     * @return the query plan with all the interconnected logical operators
     */
    QueryPlanPtr getQueryPlan();

    /**
     * @brief Mark the global query as deployed
     */
    void markAsDeployed();

    /**
     * @brief Mark the global query as not deployed
     */
    void markAsNotDeployed();

    /**
     * @brief Is the query plan for the metadata deployed
     * @return true if deployed else false
     */
    bool isDeployed() const;

    /**
     * @brief Check if the metadata is empty
     * @return true if vector of queryIds is empty
     */
    bool isEmpty();

    /**
     * @brief Check if the metadata is newly created
     * A New metadata is the one which was created by never got deployed before
     * @return true if newly created else false
     */
    bool isNew() const;

    /**
     * @brief Get the set of Global Query Nodes with sink operators grouped together
     * @return the set of Global Query Nodes with Sink Operators
     */
    std::set<GlobalQueryNodePtr> getSinkGlobalQueryNodes();

    /**
     * @brief Get the collection of registered query ids
     * @return set of registered query ids
     */
    std::set<QueryId> getQueryIds();

    /**
     * @brief Get the global query id
     * @return global query id
     */
    GlobalQueryId getGlobalQueryId() const;

    /**
     * @brief Set the meta data as old
     * An Old metadata is deployed at least once in his life time.
     */
    void setAsOld();

  private:
    explicit GlobalQueryMetaData(std::set<QueryId> queryIds, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes);

    GlobalQueryId globalQueryId;
    std::set<QueryId> queryIds;
    std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes;
    bool deployed;
    bool newMetaData;
};
}// namespace NES

#endif//NES_GLOBALQUERYMETADATA_HPP
