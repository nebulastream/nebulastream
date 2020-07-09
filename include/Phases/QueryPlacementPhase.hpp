#ifndef NES_QUERYPLACEMENTPHASE_HPP
#define NES_QUERYPLACEMENTPHASE_HPP

#include <memory>

namespace NES {

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class QueryPlacementPhase {
  public:
    QueryPlacementPhasePtr create();

    bool execute(std::string placementStrategy, QueryPlanPtr queryPlan);

  private:
    explicit QueryPlacementPhase();
};
}// namespace NES
#endif//NES_QUERYPLACEMENTPHASE_HPP
