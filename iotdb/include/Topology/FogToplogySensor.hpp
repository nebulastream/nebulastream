#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_

class FogToplogySensor{
    public:
		FogToplogySensor(size_t pSensorID)
		{
			sensorID = pSensorID;
		}

//		void addParentNode(FogToplogyNodePtr ptr)
//		{
//			parents.push_back(ptr);
//		}
		void setSensorID(size_t id)
		{
			sensorID = id;
		}
		size_t getSensorId()
		{
			return sensorID;
		}


    private:
        /** \brief stores the fog nodes this fog node transmit data to */
//        std::vector<std::weak_ptr<FogToplogyNode>> parents;
        /** \brief stores the query sub-graph processed on this node */
//        LogicalQueryGraphPtr query_graph;
        FogTopologyNodePropertiesPtr properties;
        size_t sensorID;
    };



typedef std::shared_ptr<FogToplogySensor> FogToplogySensorPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYSENSOR_HPP_ */
