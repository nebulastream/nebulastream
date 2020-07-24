#ifndef NES_GLOBALQUERYPLAN_HPP
#define NES_GLOBALQUERYPLAN_HPP

namespace NES{
/**
 * @brief This class is responsible for storing all currently running and to be deployed QueryPlans in the NES system.
 * The QueryPlans included in the GlobalQueryPlan can be fused together and therefore each operator in GQP contains
 * information about the set of queries it belongs to. The QueryPlans are bound together by a dummy logical root operator.
 */
class GlobalQueryPlan {
  public:
  private:


};
}
#endif//NES_GLOBALQUERYPLAN_HPP
