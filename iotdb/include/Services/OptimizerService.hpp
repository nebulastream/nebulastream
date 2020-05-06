#ifndef IMPL_SERVICES_OPTIMIZERSERVICE_H_
#define IMPL_SERVICES_OPTIMIZERSERVICE_H_

#include <API/InputQuery.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <cpprest/json.h>
#include <string>

namespace NES {

class OptimizerService;
typedef std::shared_ptr<OptimizerService> OptimizerServicePtr;

class OptimizerService {
  public:
    static OptimizerServicePtr getInstance() {
        static OptimizerServicePtr instance{new OptimizerService};
        return instance;
    };

    /**
     * @brief: get execution plan as json.
     *
     * @param userQuery
     * @param optimizationStrategyName
     * @return
     */
    web::json::value getExecutionPlanAsJson(InputQueryPtr inputQuery, std::string optimizationStrategyName);

    /**
     * @brief: get execution plan for the input query using the specified strategy.
     * @param userQuery
     * @param optimizationStrategyName
     * @return pointer to nes execution plan
     */
    NESExecutionPlanPtr getExecutionPlan(InputQueryPtr inputQuery, std::string optimizationStrategyName);

    ~OptimizerService() = default;

  private:
    OptimizerService() = default;
};
}// namespace NES

#endif//IMPL_SERVICES_OPTIMIZERSERVICE_H_
