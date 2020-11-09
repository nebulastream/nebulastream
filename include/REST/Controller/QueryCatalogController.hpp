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

#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>

namespace NES {
class NesCoordinator;
typedef std::weak_ptr<NesCoordinator> NesCoordinatorWeakPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class QueryCatalogController : public BaseController {

  public:
    QueryCatalogController(QueryCatalogPtr queryCatalog, NesCoordinatorWeakPtr coordinator, GlobalQueryPlanPtr globalQueryPlan);

    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryCatalogPtr queryCatalog;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
};

typedef std::shared_ptr<QueryCatalogController> QueryCatalogControllerPtr;
}// namespace NES
#endif//NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
