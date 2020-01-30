#ifndef IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IMPL_SERVICES_OPTIMIZERSERVICE_H_

#include <string>
#include <cpprest/json.h>
#include <API/InputQuery.hpp>
#include <Optimizer/NESExecutionPlan.hpp>

namespace NES {

class OptimizerService {
  public:

    static OptimizerService& instance();

    /**
     * @brief: get execution plan as json.
     *
     * @param userQuery
     * @param optimizationStrategyName
     * @return
     */
    web::json::value getExecutionPlanAsJson(InputQueryPtr inputQuery,
                                            std::string optimizationStrategyName);

    /**
   * @brief: get execution plan for the input query using the specified strategy.
   * @param userQuery
   * @param optimizationStrategyName
   * @return nes execution plan
   */
    NESExecutionPlanPtr getExecutionPlan(InputQueryPtr inputQuery,
                                         std::string optimizationStrategyName);
};
}

#endif //IMPL_SERVICES_OPTIMIZERSERVICE_H_
