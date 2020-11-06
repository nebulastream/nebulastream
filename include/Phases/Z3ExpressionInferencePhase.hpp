#ifndef NES_Z3EXPRESSIONINFERENCEPHASE_HPP
#define NES_Z3EXPRESSIONINFERENCEPHASE_HPP

#include <memory>

namespace NES{
class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;
}

namespace NES::Optimizer {

class Z3ExpressionInferencePhase;
typedef std::shared_ptr<Z3ExpressionInferencePhase> Z3ExpressionInferencePhasePtr;

/**
 * @brief This class is responsible for computing the Z3 expression for all operators within a query
 */
class Z3ExpressionInferencePhase {

  public:

    /**
     * @brief Create instance of Z3ExpressionInferencePhase class
     * @return shared instance of the Z3ExpressionInferencePhase
     */
    Z3ExpressionInferencePhasePtr create();

    /**
     * @brief this method will compute the Z3 expression for all operators of the input query plan
     * @param queryPlan: the input query plan
     */
    void execute(QueryPlanPtr queryPlan);

  private:

    explicit Z3ExpressionInferencePhase();

    ~Z3ExpressionInferencePhase();

};
}// namespace NES::Optimizer

#endif//NES_Z3EXPRESSIONINFERENCEPHASE_HPP
