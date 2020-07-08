#ifndef NES_QUERYOPTIMIZATIONPHASE_HPP
#define NES_QUERYOPTIMIZATIONPHASE_HPP

#include <memory>

namespace NES{

class QueryOptimizationPhase;
typedef std::shared_ptr<QueryOptimizationPhase> QueryOptimizationPhasePtr;

class QueryCatalogEntry;
typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;


/**
 * @brief This phase is responsible for performing query plan rewrites and query plan placement
 */
class QueryOptimizationPhase {

  public:
    static QueryOptimizationPhasePtr create();

    /**
     * @brief Perform query optimization steps on the input query catalog entry
     * @param queryCatalogEntry : query catalog entry containing the information about the query to be optimized
     * @return true if optimization succeeds
     */
    bool execute(QueryCatalogEntryPtr queryCatalogEntry);

  private:
    explicit QueryOptimizationPhase();

};
}
#endif//NES_QUERYOPTIMIZATIONPHASE_HPP
