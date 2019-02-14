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

	FogExecutionPlanPtr map(InputQuery& query, FogTopologyPlanPtr plan)
	{
		//Query graph auf root node
		//copy and override pointer to node
	}

	void optimize(FogExecutionPlanPtr execPlan)
	{

	}

};
}


#endif /* INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_ */
