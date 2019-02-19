#ifndef INCLUDE_OPTIMIZER_FOGEXECUTIONPLAN_HPP_
#define INCLUDE_OPTIMIZER_FOGEXECUTIONPLAN_HPP_
#include <memory>
namespace iotdb{
class FogExecutionPlan
{
public:
	FogExecutionPlan(){};
};


typedef std::shared_ptr<FogExecutionPlan> FogExecutionPlanPtr;


}
#endif /* INCLUDE_OPTIMIZER_FOGEXECUTIONPLAN_HPP_ */
