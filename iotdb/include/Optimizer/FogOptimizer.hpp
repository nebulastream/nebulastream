#ifndef INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_
#define INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_

#include <API/InputQuery.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/FogExecutionPlan.hpp>


namespace iotdb{
class FogOptimizer
{
public:
	FogOptimizer(){};

	FogExecutionPlanPtr map(InputQueryPtr query, FogTopologyPlanPtr plan)
	{
		FogExecutionPlanPtr execPlan = std::make_shared<FogExecutionPlan>();

		//get Root
		FogTopologyWorkerNodePtr rootNode = plan->getRootNode();
		rootNode->setQuery(query);

		return execPlan;
	}

	void optimize(FogExecutionPlanPtr execPlan)
	{

	}

};
}


#endif /* INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_ */
