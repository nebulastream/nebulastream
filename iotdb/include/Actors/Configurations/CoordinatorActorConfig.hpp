#ifndef __ACTOR_COORDINATOR_CONFIG_HPP__
#define __ACTOR_COORDINATOR_CONFIG_HPP__

#undef U
#include <Util/Logger.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>

namespace NES {
/**
* @brief The configuration file of the coordinator
*/
class CoordinatorActorConfig : public caf::actor_system_config {
  public:
    CoordinatorActorConfig(const CoordinatorActorConfig& config) {
        ip = config.ip;
        publish_port = config.publish_port;
        receive_port = config.receive_port;
        opt_group{custom_options_, "global"}
            .add(ip, "ip", "set ip")
            .add(publish_port, "publish_port,ppub", "set publish_port")
            .add(receive_port, "receive_port,prec", "set receive_port");
    }

    CoordinatorActorConfig() {
        opt_group{custom_options_, "global"}
            .add(ip, "ip", "set ip")
            .add(publish_port, "publish_port,ppub", "set publish_port")
            .add(receive_port, "receive_port,prec", "set receive_port");
    }

    void printCfg() {
        NES_DEBUG(" ip=" << ip << " receive_port=" << receive_port
                         << " publish_port=" << publish_port);
    }

    std::string ip = "127.0.0.1";
    uint16_t publish_port = 4715;//ZMQ?
    uint16_t receive_port = 4815;//Actors
};
}// namespace NES
#endif//__ACTOR_COORDINATOR_CONFIG_HPP__
