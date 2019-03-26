#ifndef INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_
#define INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_

#include <API/InputQuery.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/FogExecutionPlan.hpp>


namespace iotdb{
class FogOptimizer
{
public:
	static FogOptimizer& getInstance()
	{
			   static FogOptimizer instance; // Guaranteed to be destroyed.
									 // Instantiated on first use.
			   return instance;
	}
	FogOptimizer(FogOptimizer const&);// Don't Implement
	void operator=(FogOptimizer const&); // Don't implement

	FogExecutionPlanPtr map(InputQueryPtr query, FogTopologyPlanPtr plan)
	{
		FogExecutionPlanPtr execPlan = std::make_shared<FogExecutionPlan>();

		//get Root
		FogTopologyEntryPtr rootNode = plan->getRootNode();
		rootNode->setQuery(query);

		return execPlan;
	}

	void optimize(FogExecutionPlanPtr execPlan)
	{
		//TODO: do the magic

	}
private:
	FogOptimizer(){};

};
}


#endif /* INCLUDE_OPTIMIZER_FOGOPTIMIZER_HPP_ */
