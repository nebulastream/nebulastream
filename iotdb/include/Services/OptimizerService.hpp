#ifndef IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_

#include <string>
#include <Optimizer/FogExecutionPlan.hpp>
#include <API/InputQuery.hpp>

namespace iotdb {

class OptimizerService {

 public:

  /**
 * @brief: get execution plan.
 *
 * @param userQuery
 * @param optimizationStrategyName
 * @return
 */
  FogExecutionPlan getExecutionPlan(InputQueryPtr inputQuery, std::string optimizationStrategyName);

};
}

#endif //IOTDB_IMPL_SERVICES_OPTIMIZERSERVICE_H_
