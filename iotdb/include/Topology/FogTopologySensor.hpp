#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include <memory>

#define INVALID_NODE_ID 101

class FogToplogySensor{

    public:
		FogToplogySensor()
		{
			sensorID = INVALID_NODE_ID;
		}

		void setSensorID(size_t id)
		{
			sensorID = id;
		}
		size_t getSensorId()
		{
			return sensorID;
		}


    private:
        size_t sensorID;
    };



typedef std::shared_ptr<FogToplogySensor> FogToplogySensorPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */
