#include "REST/Controller/QueryCatalogController.hpp"

namespace NES {

void QueryCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {
    if (path[1] == "queries") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        std::string queryStatus = req.at("status").as_string();

                        //Prepare the response
                        json::value result{};
                        map<string, string> queries = queryCatalogService.getQueriesWithStatus(queryStatus);

                        for (auto[key, value] :  queries) {
                            result[key] = json::value::string(value);
                        }

                        successMessageImpl(message, result);
                        return;
                    } catch (...) {
                        std::cout << "Exception occurred while building the query plan for user request.";
                        internalServerErrorImpl(message);
                        return;
                    }
                  }
            )
            .wait();

    } else if (path[1] == "allRegisteredQueries") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        std::string queryStatus = req.at("status").as_string();

                        //Prepare the response
                        json::value result{};
                        map<string, string> queries = queryCatalogService.getAllRegisteredQueries();

                        for (auto[key, value] :  queries) {
                            result[key] = json::value::string(value);
                        }

                        successMessageImpl(message, result);
                        return;
                    } catch (...) {
                        std::cout << "Exception occurred while building the query plan for user request.";
                        internalServerErrorImpl(message);
                        return;
                    }
                  }
            )
            .wait();
    }

    resourceNotFoundImpl(message);
}

void QueryCatalogController::setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->coordinatorActorHandle = coordinatorActorHandle;
}

void QueryCatalogController::handleDelete(std::vector<utility::string_t> path, web::http::http_request message) {

}

}
