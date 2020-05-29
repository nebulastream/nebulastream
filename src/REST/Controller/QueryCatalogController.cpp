#include "REST/Controller/QueryCatalogController.hpp"
#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryCatalogController::QueryCatalogController(QueryCatalogPtr queryCatalog,
                                               NesCoordinatorPtr coordinator) {
    NES_DEBUG("QueryCatalogController()");
}

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
                    map<string, string> queries = queryCatalog->getQueriesWithStatus(queryStatus);

                    for (auto [key, value] : queries) {
                        result[key] = json::value::string(value);
                    }

                    if (queries.size() == 0) {
                        NES_DEBUG(
                            "QueryCatalogController: handleGet -queries: no registered query with status " + queryStatus
                            + " was found.");
                        noContentImpl(message);
                    } else {
                        successMessageImpl(message, result);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -queries: Exception occurred while building the query plan for user request:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();

    } else if (path[1] == "allRegisteredQueries") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare the response
                    json::value result{};
                    map<string, string> queries = queryCatalog->getAllRegisteredQueries();

                    for (auto [key, value] : queries) {
                        result[key] = json::value::string(value);
                    }

                    if (queries.size() == 0) {
                        NES_DEBUG("QueryCatalogController: handleGet -queries: no registered query was found.");
                        noContentImpl(message);
                    } else {
                        successMessageImpl(message, result);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -allRegisteredQueries: Exception occurred while building the query plan for user request:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    }

    resourceNotFoundImpl(message);
}

void QueryCatalogController::handleDelete(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "query") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    json::value req = json::value::parse(payload);
                    std::string queryId = req.at("queryId").as_string();

                    //Perform async call for deleting the query using actor
                    //Note: This is an async call and would not know if the deletion has failed
                    bool success = coordinator->removeQuery(queryId);

                    if (success) {
                        //Prepare the response
                        json::value result{};
                        result["success"] = json::value::boolean(success);
                        successMessageImpl(message, result);
                    } else {
                        throw std::invalid_argument("Could not delete query with id " + queryId + ".");
                    }

                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleDelete -query: Exception occurred while building the query plan for user request:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    }

    resourceNotFoundImpl(message);
}

}// namespace NES
