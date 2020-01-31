#ifndef INCLUDE_COMPONENTS_NESWORKER_HPP_
#define INCLUDE_COMPONENTS_NESWORKER_HPP_
#include <Actors/WorkerActor.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <Actors/AtomUtils.hpp>

using namespace std;

namespace NES {

class NesWorker {
 public:
  NesWorker() {
    workerCfg.load<io::middleman>();
    actor_system system { workerCfg };
    client = system.spawn<NES::WorkerActor>(workerCfg.ip,
                                            workerCfg.publish_port,
                                            workerCfg.receive_port);
    connected = false;

    connect();
  }

  bool connect(std::string host, uint16_t port) {
    WorkerActorConfig workerCfg;
    workerCfg.host = host;
    workerCfg.publish_port = port;
    workerCfg.load<io::middleman>();

    actor_system actorSystem { workerCfg };
    scoped_actor self { actorSystem };
    bool &con = this->connected;
    self->request(client, task_timeout, connect_atom::value, workerCfg.host,
                  workerCfg.publish_port).receive(
        [&con, &workerCfg](const bool &c) mutable {
          con = c;
          NES_DEBUG(
              "NESWORKER: connected successfully to host " << workerCfg.host << " port=" << workerCfg.publish_port)
        }
        ,
        [=](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR("NESWORKER: ERROR while try to connect " << error_msg)
        });
    return con;
  }

  bool connect() {
    WorkerActorConfig workerCfg;
    workerCfg.load<io::middleman>();
    actor_system actorSystem { workerCfg };
    scoped_actor self { actorSystem };
    bool &con = this->connected;
    self->request(client, task_timeout, connect_atom::value, workerCfg.host,
                  workerCfg.publish_port).receive(
        [&con, &workerCfg](const bool &c) mutable {
          con = c;
          NES_DEBUG(
              "NESWORKER: connected successfully to host " << workerCfg.host << " port=" << workerCfg.publish_port)
        }
        ,
        [=](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR("NESWORKER: ERROR while try to connect " << error_msg)
        });
    return con;
  }

  bool disconnect() {
    WorkerActorConfig workerCfg;
    workerCfg.load<io::middleman>();
    actor_system actorSystem { workerCfg };
    scoped_actor self { actorSystem };
    bool &con = this->connected;

    self->request(client, task_timeout, disconnect_atom::value).receive(
        [&con](const bool &dc) mutable {
          con = false;
          NES_DEBUG("NESWORKER: disconnected successfully")
        }
        ,
        [=](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR("NESWORKER: ERROR while try to disconnect " << error_msg)
        });
    return !con;
  }

  bool registerLogicalStream(std::string name, std::string path) {
    WorkerActorConfig workerCfg;
    workerCfg.load<io::middleman>();
    actor_system actorSystem { workerCfg };
    scoped_actor self { actorSystem };

    bool success = false;

    self->request(client, task_timeout, register_log_stream_atom::value, name,
                  path).receive(
        [&success](const bool &dc) mutable {
          NES_DEBUG("NESWORKER: register log stream successful")
          success = true;
        }
        ,
        [=,&success](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "NESWORKER: ERROR while try to register stream" << error_msg)
          success = false;
        });
    return success;
  }

  bool registerPhysicalStream(std::string sourceType, std::string sourceConf,
                              std::string physicalStreamName,
                              std::string logicalStreamName) {
    WorkerActorConfig workerCfg;
    workerCfg.load<io::middleman>();
    actor_system actorSystem { workerCfg };
    scoped_actor self { actorSystem };

    bool success = false;

    self->request(client, task_timeout, register_phy_stream_atom::value,
                  sourceType, sourceConf, physicalStreamName, logicalStreamName)
        .receive(
        [&success](const bool &dc) mutable {
          NES_DEBUG("NESWORKER: register log stream successful")
          success = true;
        }
        ,
        [=,&success](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "NESWORKER: ERROR while try to register stream" << error_msg)
          success = false;
        });
    return success;
  }

 private:
  static void setupLogging();
  bool connected;
  infer_handle_from_class_t<NES::WorkerActor> client;
  WorkerActorConfig workerCfg;
  PhysicalStreamConfig defaultConf;

};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */
