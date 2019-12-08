//
// Created by achaudhary on 23.07.19.
//

#ifndef IOTDB_FOGTOPOLOGYSERVICE_HPP
#define IOTDB_FOGTOPOLOGYSERVICE_HPP

#include <cpprest/json.h>

namespace iotdb {

    /**\brief:
    *          This class is used for serving different requests related to Fog topology.
    *
    * Note: Please extend the header if new topology related functionality needed to be added.
    *
    */
    class FogTopologyService {

    private:

    public:

        /**
         * This method is used for getting the fog topology as json string.
         *
         * @return a json object representing the fog topology
         */
        web::json::value getFogTopologyAsJson();
    };
}


#endif //IOTDB_FOGTOPOLOGYSERVICE_HPP
