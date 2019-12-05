
#ifndef IOTDB_INCLUDE_ACTORS_CONFIGURATIONS_ACTORWORKERCONFIG_H_
#define IOTDB_INCLUDE_ACTORS_CONFIGURATIONS_ACTORWORKERCONFIG_H_

#include <caf/all.hpp>

namespace iotdb {
/**
* @brief The configuration for worker
*/
class ActorWorkerConfig : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t receive_port = 0;
  std::string host = "localhost";
  uint16_t publish_port = 4711;
  std::string sensor_type = "cars";

  ActorWorkerConfig() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port")
        .add(host, "host,H", "set host (ignored in server mode)")
        .add(sensor_type, "sensor_type", "set sensor_type");
  }
};

}

#endif //IOTDB_INCLUDE_ACTORS_CONFIGURATIONS_ACTORWORKERCONFIG_H_
