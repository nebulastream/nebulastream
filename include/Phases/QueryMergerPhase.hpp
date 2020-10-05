#ifndef NES_QUERYMERGERPHASE_HPP
#define NES_QUERYMERGERPHASE_HPP

#include <memory>

namespace NES {

class QueryMergerPhase;
typedef std::shared_ptr<QueryMergerPhase> QueryMergerPhasePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class L0QueryMergerRule;
typedef std::shared_ptr<L0QueryMergerRule> L0QueryMergerRulePtr;

class QueryMergerPhase {

  public:
    static QueryMergerPhasePtr create();

    /**
     * @brief
     * @param globalQueryPlan:
     * @return true if successful
     */
    bool execute(GlobalQueryPlanPtr globalQueryPlan);

  private:
    explicit QueryMergerPhase();

    L0QueryMergerRulePtr l0QueryMergerRule;
};
}// namespace NES
#endif//NES_QUERYMERGERPHASE_HPP
