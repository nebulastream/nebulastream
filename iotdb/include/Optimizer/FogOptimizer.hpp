#ifndef INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_
#define INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_

#include <API/InputQuery.hpp>
#include <Topology/FogTopologyPlan.hpp>

namespace iotdb{
class FogOptimizer
{
public:
	FogOptimizer(){};

	void optimize(InputQuery& query, FogTopologyPlan* plan)
	{

	}

};
}


#endif /* INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_ */
