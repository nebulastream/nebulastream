#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYNODE_HPP_

#include <vector>
#include <memory>

#include "FogTopologyNodeProperties.hpp"

class FogToplogyNode;
typedef std::shared_ptr<FogToplogyNode> FogToplogyNodePtr;


class FogToplogyNode{
    public:
		FogToplogyNode(size_t pNode_id)
		{
			node_id = pNode_id;
		}

		void addChildNode(FogToplogyNodePtr ptr)
		{
			childs.push_back(ptr);
		}
		void addParentNode(FogToplogyNodePtr ptr)
		{
			parents.push_back(ptr);
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
        /** \brief stores the fog nodes this fog node receives its data from */
        std::vector<FogToplogyNodePtr> childs;
        /** \brief stores the fog nodes this fog node transmit data to */
        std::vector<std::weak_ptr<FogToplogyNode>> parents;
        /** \brief stores the query sub-graph processed on this node */
//        LogicalQueryGraphPtr query_graph;
        FogTopologyNodePropertiesPtr properties;
        size_t node_id;
    };



#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYNODE_HPP_ */
