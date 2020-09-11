#ifndef NES_L0QUERYMERGERRULE_HPP
#define NES_L0QUERYMERGERRULE_HPP

#include <memory>
#include <vector>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class L0QueryMergerRule;
typedef std::shared_ptr<L0QueryMergerRule> L0QueryMergerRulePtr;

/**
 * @brief L0QueryMergerRule is responsible for merging together all the equivalent chains of Global Query Nodes within the Global Query Plan such that, after running this rule
 * all equivalent operator chains should be merged together and only a single representative operator chain should exists in the Global Query Plan for all of them.
 * Effectively this will prune the global query plan size.
 *
 * Following are the conditions for the two global query node chains to be equivalent:
 *  - For each global query node in the first chain, there should exists an equal global query node in the other chain (except for the node with the sink operator).
 *      - For two global query nodes to be equal, we check that for each logical operator in one global query node their is an equivalent logical operator in the other
 *      global query node.
 *  - The order of global query nodes in both the chains should be same.
 *
 * Following is the example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter1},{Q1})    GQN7({Filter1},{Q1})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * After running the L0QueryMergerRule, the resulting Global Query Plan will look as follow:
 *
 *                                                         GQPRoot
 *                                                         /     \
 *                                                        /       \
 *                                           GQN1({Sink1},{Q1}) GQN5({Sink2},{Q2})
 *                                                        \      /
 *                                                         \   /
 *                                                   GQN2({Map1},{Q1,Q2})
 *                                                           |
 *                                                  GQN3({Filter1},{Q1,Q2})
 *                                                           |
 *                                                GQN4({Source(Car)},{Q1,Q2})
 *
 */
class L0QueryMergerRule {

  public:
    static L0QueryMergerRulePtr create();

    /**
     * @brief apply L0QueryMerger rule on the globalQuery plan
     * @param queryPlans: the input query plans to merge
     * @param globalQueryPlan: the global query plan
     */
    void apply(std::vector<QueryPlanPtr> queryPlans, GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit L0QueryMergerRule();

};
}// namespace NES
#endif//NES_L0QUERYMERGERRULE_HPP
