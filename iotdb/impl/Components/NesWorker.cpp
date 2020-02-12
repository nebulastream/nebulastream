#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include "Actors/WorkerActor.hpp"
#include <stdlib.h>
#include <signal.h> //  our new library

volatile sig_atomic_t flag = 0;
void termFunc(int sig) {
  flag = 1;
}

namespace NES {

NesWorker::NesWorker() {
  connected = false;
  coordinatorPort = 0;
  NES_DEBUG("NesWorker: constructed")

  // Register signals
  signal(SIGINT, termFunc);
}

bool NesWorker::start(bool blocking, bool withConnect, uint16_t port) {
  NES_DEBUG("NesWorker: start with blocking " << blocking << " port=" << port)
  WorkerActorConfig w_cfg;
  w_cfg.load<io::middleman>();
  w_cfg.printCfg();
  actor_system sw { w_cfg };
  PhysicalStreamConfig streamConf;
  auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                           w_cfg.receive_port);

  workerCfg.load<io::middleman>();
  coordinatorPort = port;
  actorSystem = new actor_system { workerCfg };

  workerHandle = actorSystem->spawn<NES::WorkerActor>(workerCfg.ip,
                                                      workerCfg.publish_port,
                                                      workerCfg.receive_port);
  if (withConnect) {
    NES_DEBUG("NesWorker: start with connect")
    connect();
  }
  if (blocking) {
    NES_DEBUG("NesWorker: started, join now and waiting for work")
    while (true) {
      if (flag) {
        NES_DEBUG("NesWorker: caught signal terminating worker")
        flag = 0;
        break;
      } else {
        cout << "NesWorker wait" << endl;
        sleep(5);
      }
    }
    NES_DEBUG("NesWorker: joined, return")
    return true;
  } else {
    NES_DEBUG("NesWorker: started, return without blocking")
    return true;
  }
}

bool NesWorker::stop() {
  NES_DEBUG("NesWorker: stop")
  scoped_actor self { *actorSystem };
  //TODO: what is the return type here?
  self->request(workerHandle, task_timeout, exit_reason::user_shutdown);
  return true;
}

bool NesWorker::connect() {
  scoped_actor self { *actorSystem };
  bool &con = this->connected;
  NES_DEBUG(
      "NesWorker: worker connect to host=" << workerCfg.host << " port=" << coordinatorPort)
  self->request(workerHandle, task_timeout, connect_atom::value, workerCfg.host,
                coordinatorPort).receive(
      [&con](const bool &c) mutable {
        con = c;
        NES_DEBUG("NesWorker: connected successfully")
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR("NesWorker: ERROR while try to connect " << error_msg)
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
