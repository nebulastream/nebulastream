#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>

namespace NES {

NesWorker::NesWorker() {
  workerCfg.load<io::middleman>();
  actor_system system { workerCfg };
  client = system.spawn<NES::WorkerActor>(workerCfg.ip, workerCfg.publish_port,
                                          workerCfg.receive_port);
  connected = false;

  connect();
}

bool NesWorker::connect(std::string host, uint16_t port)
{
  NES_NOT_IMPLEMENTED
}


bool NesWorker::connect() {
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

bool NesWorker::disconnect() {
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

bool NesWorker::registerLogicalStream(std::string name, std::string path) {
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
      [=, &success](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR("NESWORKER: ERROR while try to register stream" << error_msg)
        success = false;
      });
  return success;
}

bool NesWorker::registerPhysicalStream(std::string sourceType, std::string sourceConf,
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
      [=, &success](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR("NESWORKER: ERROR while try to register stream" << error_msg)
        success = false;
      });
  return success;
}

}
