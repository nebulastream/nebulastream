#ifndef IOTDB_NESTOPOLOGYSERVICE_HPP
#define IOTDB_NESTOPOLOGYSERVICE_HPP

#include <cpprest/json.h>
//#include <Topology/TestTopology.hpp>

namespace iotdb {

    /**\brief:
    *          This class is used for serving different requests related to nes topology.
    *
    * Note: Please extend the header if new topology related functionality needed to be added.
    *
    */
    class NESTopologyService {

    private:

    public:

        /**
         * This method is used for getting the nes topology as json string.
         *
         * @return a json object representing the nes topology
         */
        web::json::value getNESTopologyAsJson();
    };
}


#endif //IOTDB_NESTOPOLOGYSERVICE_HPP
