#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_

#include <vector>
#include <memory>
#define INVALID_NODE_ID 101

class FogTopologyNode;
typedef std::shared_ptr<FogTopologyNode> FogTopologyNodePtr;


class FogTopologyNode{

    public:
		FogTopologyNode()
		{
			node_id = INVALID_NODE_ID;
		}

		void setNodeId(size_t id)
		{
			node_id = id;
		}

		size_t getNodeId()
		{
			return node_id;
		}

    private:
        size_t node_id;
    };



#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYNODE_HPP_ */