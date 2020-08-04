#ifndef NES_GLOBALQUERYPLANUPDATEPHASE_HPP
#define NES_GLOBALQUERYPLANUPDATEPHASE_HPP

#include <memory>
#include <vector>

namespace NES {

class QueryCatalogEntry;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class GlobalQueryPlanUpdatePhase;
typedef std::shared_ptr<GlobalQueryPlanUpdatePhase> QueryMergerPhasePtr;

/**
 * @brief This class is responsible for accepting a batch of query requests and then updating the Global Query Plan accordingly.
 */
class GlobalQueryPlanUpdatePhase {
  public:
    /**
     * @brief Create an instance of the GlobalQueryPlanUpdatePhase
     * @return Shared pointer for the GlobalQueryPlanUpdatePhase
     */
    static QueryMergerPhasePtr create();

    /**
     * @brief This method executes the Global Query Plan Update Phase on a batch of query requests
     * @param queryRequests: a batch of query requests (in the form of Query Catalog Entry) to be processed to update global query plan
     * @return Shared pointer to the Global Query Plan for further processing
     */
    GlobalQueryPlanPtr execute(const std::vector<QueryCatalogEntry> queryRequests);

  private:
    GlobalQueryPlanUpdatePhase();

    GlobalQueryPlanPtr globalQueryPlan;
};
}// namespace NES

#endif//NES_GLOBALQUERYPLANUPDATEPHASE_HPP
