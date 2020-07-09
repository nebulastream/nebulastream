#ifndef NES_QUERYOPTIMIZATIONPHASE_HPP
#define NES_QUERYOPTIMIZATIONPHASE_HPP

#include <memory>

namespace NES {

class QueryOptimizationPhase;
typedef std::shared_ptr<QueryOptimizationPhase> QueryOptimizationPhasePtr;

class QueryCatalogEntry;
typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

/**
 * @brief This phase is responsible for performing query plan rewrites and query plan placement
 */
class QueryOptimizationPhase {

  public:

    /**
     * @brief Create Query Optimization phase shared pointer
     * @param queryRewritePhase : query re-write phase
     * @param typeInferencePhase : type inference phase
     * @param queryPlacementPhase : query placement phase
     * @return Query Optimization phase Shared pointer
     */
    static QueryOptimizationPhasePtr create(QueryRewritePhasePtr queryRewritePhase, TypeInferencePhasePtr typeInferencePhase,
                                            QueryPlacementPhasePtr queryPlacementPhase);

    /**
     * @brief Perform query optimization steps on the input query catalog entry
     * @param queryCatalogEntry : query catalog entry containing the information about the query to be optimized
     * @return true if optimization succeeds
     */
    bool execute(QueryCatalogEntryPtr queryCatalogEntry);

  private:
    explicit QueryOptimizationPhase(QueryRewritePhasePtr queryRewritePhase, TypeInferencePhasePtr typeInferencePhase,
                                    QueryPlacementPhasePtr queryPlacementPhase);
    TypeInferencePhasePtr typeInferencePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
    QueryRewritePhasePtr queryRewritePhase;
};
}// namespace NES
#endif//NES_QUERYOPTIMIZATIONPHASE_HPP
