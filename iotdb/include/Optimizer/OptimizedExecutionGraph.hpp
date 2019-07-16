#ifndef IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
#define IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP

#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>

namespace iotdb {

    struct Vertex {
        size_t id;
        ExecutionNodePtr ptr;
    };
    struct Edge {
        size_t id;
        ExecutionNodeLinkPtr ptr;
    };

    class ExecutionGraph {

    public:


    };

    class OptimizedExecutionGraph {



    };
};

#endif //IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
