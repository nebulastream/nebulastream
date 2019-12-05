
#ifndef IOTDB_INCLUDE_ACTORS_CAFSERVER_H_
#define IOTDB_INCLUDE_ACTORS_CAFSERVER_H_

#include <iostream>
#include <caf/io/all.hpp>
#include <caf/all.hpp>
#include <Actors/Configurations/ActorCoordinatorConfig.hpp>

namespace iotdb {

/**
 * @brief : This class is responsible for starting the CAF server with coordinator services.
 */

class CAFServer {

 public:
  bool start(actor_system& actor_system, const ActorCoordinatorConfig& cfg);

 private:
  void setupLogging();
};

}

#endif
