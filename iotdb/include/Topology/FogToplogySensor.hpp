/*
 * FogToplogySensor.hpp
 *
 *  Created on: Jan 17, 2019
 *      Author: zeuchste
 */

#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_

class FogToplogySensor{
    public:
		FogToplogySensor()
		{

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
        /** \brief stores the fog nodes this fog node transmit data to */
        std::vector<std::weak_ptr<FogToplogyNode>> parents;
        /** \brief stores the query sub-graph processed on this node */
//        LogicalQueryGraphPtr query_graph;
        FogNodePropertiesPtr properties;
        size_t node_id;
    };



typedef std::shared_ptr<FogToplogySensor> FogToplogySensorPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_ */
