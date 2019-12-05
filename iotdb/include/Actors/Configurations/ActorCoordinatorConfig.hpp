
#ifndef IOTDB_IMPL_ACTORS_COORDINATORCONFIGURATION_H_
#define IOTDB_IMPL_ACTORS_COORDINATORCONFIGURATION_H_

#include <caf/all.hpp>

namespace iotdb {
/**
* @brief The configuration file of the coordinator
*/
class ActorCoordinatorConfig : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t publish_port = 4711;
  uint16_t receive_port = 4815;

  ActorCoordinatorConfig() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port");
  }
};
}
#endif //IOTDB_IMPL_ACTORS_COORDINATORCONFIGURATION_H_
