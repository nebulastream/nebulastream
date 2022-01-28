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
#include <REST/Controller/MaintenanceController.hpp>
#include <REST/runtime_utils.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
#include <Phases/MigrationType.hpp>

namespace NES::Experimental{

MaintenanceController::MaintenanceController(MaintenanceServicePtr maintenanceService) :maintenanceService{maintenanceService}{}

void MaintenanceController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& request) {
    if (path[1] == "mark") {
        NES_DEBUG("MaintenanceController: handlePost -mark: REST received request to mark node for maintenance "
                      << request.to_string());
        request.extract_string(true)
            .then([this, request](utility::string_t body) {
              try {
                  NES_DEBUG("MaintenanceController: handlePost -mark: Start trying to mark nodes for maintenance");
                  //Parse ID of node to mark for maintenance
                  std::string payload(body.begin(), body.end());
                  NES_DEBUG("MaintenanceController: handlePost -mark: userRequest: " << payload);
                  web::json::value req = web::json::value::parse(payload);
                  NES_DEBUG("MaintenanceController: handlePost -mark: Json Parse Value: " << req);
                  if (!req.has_field("id")) {
                      NES_ERROR("MaintenanceController: Unable to find Field: id ");
                      web::json::value errorResponse{};
                      errorResponse["detail"] = web::json::value::string("Field id must be provided");
                      badRequestImpl(request, errorResponse);
                      return;
                  }
                  if (!req.has_field("migrationType")) {
                      NES_ERROR("MaintenanceController: Unable to find Field: migrationType ");
                      web::json::value errorResponse{};
                      errorResponse["detail"] = web::json::value::string("Field migrationType must be provided");
                      badRequestImpl(request, errorResponse);
                      return;
                  }
                  uint64_t id = req.at("id").as_integer();
                  MigrationType::Value migrationType = MigrationType::Value(req.at("migrationType").as_integer());
                  maintenanceService->submitMaintenanceRequest(id,migrationType);
                  web::json::value result{};
                      result["Info"] = web::json::value::string("Successfully submitted Maintenance Request");
                      result["Node Id"] = web::json::value::number(id);
                      result["Migration Type"] = web::json::value::number(migrationType);
                      successMessageImpl(request, result);
                  return;

              } catch (const std::exception& exc) {
                  NES_ERROR("MaintenanceController: handlePost -mark: Exception occurred during handling of Maintenance Request"
                                << exc.what());
                  handleException(request, exc);
                  return;
              } catch (...) {
                  RuntimeUtils::printStackTrace();
                  internalServerErrorImpl(request);
                  return;
              }
            })
            .wait();
    }else {
        resourceNotFoundImpl(request);
    }
}

}//namespace NES::Experimental

