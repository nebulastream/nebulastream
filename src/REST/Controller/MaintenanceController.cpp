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
// Created by balint on 25.12.21.
//
#include <REST/Controller/MaintenanceController.hpp>
#include <REST/runtime_utils.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
namespace NES{

MaintenanceController::MaintenanceController(MaintenanceServicePtr maintenanceService) :maintenanceService{maintenanceService}{}


void MaintenanceController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_DEBUG("MaintenanceController: POST");

    if (path[1] == "mark") {


        NES_DEBUG("MaintenanceController: handlePost -mark: REST received request to mark node for maintenance "
                      << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
              try {
                  NES_DEBUG("MaintenanceController: handlePost -mark: Start trying to mark nodes for maintenance");
                  //Parse IDs of nodes to mark for maintenance
                  std::string payload(body.begin(), body.end());
                  NES_DEBUG("MaintenanceController: handlePost -mark: userRequest: " << payload);
                  web::json::value req = web::json::value::parse(payload);
                  NES_DEBUG("MaintenanceController: handlePost -mark: Json Parse Value: " << req);
                  //TODO: handle multiple IDs

                  uint64_t id = req.at("ids").as_integer();
                  uint8_t strategy = req.at("strategy").as_integer();

                  maintenanceService->submitMaintenanceRequest(id,strategy);

                  web::json::value result{};

                      result["Test"] = web::json::value::string("This is a test");
                      result["Node Id"]      =web::json::value::number(id);
                      result["Strategy"] = web::json::value::number(strategy);
                      successMessageImpl(message, result);
                  return;

              } catch (const std::exception& exc) {
                  NES_ERROR("MaintenanceController: handlePost -mark: Exception occurred while trying to mark node for maintenance "
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

