#ifndef NES_QUERYMERGERPHASE_HPP
#define NES_QUERYMERGERPHASE_HPP

#include <memory>
#include <vector>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;

/**
 * @brief This class is responsible for merging a batch of queries together with already running queries.
 * There are different level of query merging policies present in NES and depending on the system configuration the specific level of merging is called.
 */
class QueryMergerPhase {
  public:
    /**
     * @brief Create an instance of the QueryMergerPhase
     * @return Shared pointer for the query merger phase
     */
    QueryMergerPhasePtr create();

    /**
     * @brief This method executes the Query merger phase on a batch of query plans
     * @param batchOfQueries: the patch of query to be merged with existing global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(std::vector<QueryPlanPtr> batchOfQueries);

  private:
    QueryMergerPhase();

    GlobalQueryPlanPtr globalQueryPlan;
};
}// namespace NES

#endif//NES_QUERYMERGERPHASE_HPP
