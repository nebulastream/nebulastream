#ifndef NES_QUERYPLACEMENTPHASE_HPP
#define NES_QUERYPLACEMENTPHASE_HPP

#include <memory>

namespace NES {

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class NESTopologyPlan;
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryPlacementPhase {
  public:
    QueryPlacementPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan,
                                  TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);
    bool execute(std::string placementStrategy, QueryPlanPtr queryPlan);
  private:
    explicit QueryPlacementPhase(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan,
                                 TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog);

    GlobalExecutionPlanPtr globalExecutionPlan;
    NESTopologyPlanPtr nesTopologyPlan;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
};
}// namespace NES
#endif//NES_QUERYPLACEMENTPHASE_HPP
