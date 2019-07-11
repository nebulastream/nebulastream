#ifndef IOTDB_FOGTOPOLOGYGENERATOR_HPP
#define IOTDB_FOGTOPOLOGYGENERATOR_HPP

#include <Topology/FogTopologyPlan.hpp>
#include <Topology/FogTopologyManager.hpp>
#include <cpprest/json.h>

namespace iotdb {


    class FogTopologyGenerator {

    public:
        std::shared_ptr<FogTopologyPlan> generateExampleTopology();

        web::json::value getFogTopologyAsJson(const std::shared_ptr<FogTopologyPlan>* pPtr);
    };
}
#endif
