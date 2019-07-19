
/**\brief:
 *         This class is responsible for producing the execution graph for the queries on fog topology. We can have
 *         different type of optimizers that can be provided as input the query and fog topology information and based
 *         on that a query execution graph will be computed. The output will be a graph where the edges contain the
 *         information about the operators to be executed and the nodes where the execution is to be done.
 */


#ifndef IOTDB_QUERYOPTIMIZER_HPP
#define IOTDB_QUERYOPTIMIZER_HPP


namespace iotdb {

    class QueryOptimizer {

    public:
        ExecutionGraph prepareExecutionGraph(std::string strategy, InputQuery inputQuery, FogTopologyPlan fogTopologyPlan);
    };
}


#endif //IOTDB_QUERYOPTIMIZER_HPP
