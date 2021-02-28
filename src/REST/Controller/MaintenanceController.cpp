/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
//
// Created by balint on 27.02.21.
//
#include <REST/Controller/MaintenanceController.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
namespace NES{

MaintenanceController::MaintenanceController(TopologyPtr topology) :topology{topology}{};


void MaintenanceController::handlePost(std::vector<utility::string_t> paths, web::http::http_request message) {
    NES_DEBUG("MaintenanceController: POST");


    topology->print();
    if (paths[1] == "mark") {


        NES_DEBUG("TopologyController: handlePost -mark: REST received request to mark node for maintenance "
                      << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
              try {
                  NES_DEBUG("TopologyController: handlePost -mark: Start trying to mark nodes for maintenance");
                  //Parse IDs of nodes to mark for maintenance
                  std::string payload(body.begin(), body.end());
                  NES_DEBUG("TopologyController: handlePost -mark: userRequest: " << payload);
                  json::value req = json::value::parse(payload);
                  NES_DEBUG("TopologyController: handlePost -mark: Json Parse Value: " << req);
                  //TODO: handle multiple IDs

                  uint64_t id = req.at("ids").as_integer();

                  bool unmark = req.at("unmark").as_bool();

                  auto node = topology->findNodeWithId(id);

                  bool checkFlag;

                  if(unmark){
                      NES_DEBUG("TopologyController: handlePost -mark: Unmark node for maintenance: "
                                    << id);
                      //TODO: iterate over container of IDs and set all their flags
                      //TODO: make thread safe
                      node->setMaintenanceFlag(false);
                      checkFlag = node->getMaintenanceFlag();
                      //TODO: make sure all nodes have been succesfully marked
                      NES_DEBUG("TopologyController: handlePost -mark: Successfully unmarked node ?"
                                    << !checkFlag);
                      //Prepare the response
                  }
                  else {
                      NES_DEBUG("TopologyController: handlePost -mark: ID marked for maintenance " << id);
                      //TODO: iterate over container of IDs and set all their flags
                      node->setMaintenanceFlag(true);
                      checkFlag = node->getMaintenanceFlag();
                      //TODO: make sure all nodes have been succesfully marked
                      NES_DEBUG("TopologyController: handlePost -mark: Successfully marked node ?" << checkFlag);
                      //Prepare the response
                  }
                  //TODO: format for many nodes
                  json::value result{};
                  if(unmark){
                      result["Successful mark"] = json::value::boolean(!checkFlag);
                      result["Node Id"]      =json::value::number(id);
                  }
                  else {
                      result["Successful unmark"] = json::value::boolean(checkFlag);
                      result["Node Id"] = json::value::number(id);
                  }
                  successMessageImpl(message, result);
                  return;

              } catch (const std::exception& exc) {
                  NES_ERROR("TopologyController: handlePost -mark: Exception occurred while trying to mark node for maintenance "
                                << exc.what());
                  handleException(message, exc);
                  return;
              } catch (...) {
                  RuntimeUtils::printStackTrace();
                  internalServerErrorImpl(message);
                  return;
              }
            })
            .wait();

    }else {
        resourceNotFoundImpl(message);

    }
}


}




}
