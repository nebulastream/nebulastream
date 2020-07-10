#ifndef NES_QUERYOPTIMIZATIONSERVICE_HPP
#define NES_QUERYOPTIMIZATIONSERVICE_HPP

#include <memory>

namespace NES {

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryOptimizationService {
  public:
    explicit QueryOptimizationService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                      StreamCatalogPtr streamCatalog);
    int operator()();

  private:
    bool stopQueryProcessor = false;
    QueryCatalogPtr queryCatalog;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
};
}// namespace NES
#endif//NES_QUERYOPTIMIZATIONSERVICE_HPP
