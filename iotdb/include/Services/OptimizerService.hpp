#ifndef IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_

#include <string>
#include <cpprest/json.h>
#include <API/InputQuery.hpp>
#include <Optimizer/NESExecutionPlan.hpp>

namespace NES {

class OptimizerService {

 public:

  /**
   * @brief: get execution plan as json.
   *
   * @param userQuery
   * @param optimizationStrategyName
   * @return
   */
  web::json::value getExecutionPlanAsJson(InputQueryPtr inputQuery, std::string optimizationStrategyName);

  /**
 * @brief: get execution plan.
 *
 * @param userQuery
 * @param optimizationStrategyName
 * @return
 */
  NESExecutionPlan getExecutionPlan(InputQueryPtr inputQuery, std::string optimizationStrategyName);

};
}

#endif //IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_
