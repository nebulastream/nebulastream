#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include "Actors/WorkerActor.hpp"

namespace NES {

void starter(infer_handle_from_class_t<WorkerActor> handle,
             actor_system *system) {
  WorkerActorConfig workerCfg;
  workerCfg.load<io::middleman>();

  handle = system->spawn < NES::WorkerActor
      > (workerCfg.ip, workerCfg.publish_port, workerCfg.receive_port);
}

NesWorker::NesWorker() {
  connected = false;
  NES_DEBUG("NesWorker: constructed")
}

bool NesWorker::start(bool blocking) {
  NES_DEBUG("NesWorker: start with blocking " << blocking)

  workerCfg.load<io::middleman>();
  actorSystem = new actor_system { workerCfg };

  std::thread th0(starter, workerHandle, actorSystem);
  actorThread = std::move(th0);

  if (blocking) {
    NES_DEBUG("NesWorker started, join now and waiting for work")
    actorThread.join();
  } else {
    NES_DEBUG("NesWorker started, return without blocking")
    return true;
  }
}

bool NesWorker::stop() {
  NES_DEBUG("NesWorker: stop")
  actorThread.join();
}

bool NesWorker::connect() {
  WorkerActorConfig workerCfg;
  workerCfg.load<io::middleman>();

  actor_system actorSystem { workerCfg };
  scoped_actor self { actorSystem };
  bool &con = this->connected;
  self->request(workerHandle, task_timeout, connect_atom::value, workerCfg.host,
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

  self->request(workerHandle, task_timeout, disconnect_atom::value).receive(
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

  self->request(workerHandle, task_timeout, register_log_stream_atom::value,
                name, path).receive(
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

bool NesWorker::registerPhysicalStream(std::string sourceType,
                                       std::string sourceConf,
                                       std::string physicalStreamName,
                                       std::string logicalStreamName) {
  WorkerActorConfig workerCfg;
  workerCfg.load<io::middleman>();
  actor_system actorSystem { workerCfg };
  scoped_actor self { actorSystem };

  bool success = false;

  self->request(workerHandle, task_timeout, register_phy_stream_atom::value,
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
