#ifndef INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#define INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_
#include <Optimizer/FogExecutionPlan.hpp>
namespace iotdb{
class FogRunTime{
public:
	FogRunTime(){};
	void deployQuery(FogExecutionPlanPtr plan){};

private:
	void startFog();
	void stopFog();

};

}

#endif /* INCLUDE_OPTIMIZER_FOGRUNTIME_HPP_ */
