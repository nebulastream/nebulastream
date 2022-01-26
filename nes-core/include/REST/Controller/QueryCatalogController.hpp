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

#ifndef NES_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalogController : public BaseController {

  public:
    QueryCatalogController(QueryCatalogPtr queryCatalog, NesCoordinatorWeakPtr coordinator, GlobalQueryPlanPtr globalQueryPlan);

    void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) override;

  private:
    QueryCatalogPtr queryCatalog;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
};

using QueryCatalogControllerPtr = std::shared_ptr<QueryCatalogController>;
}// namespace NES
#endif  // NES_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
