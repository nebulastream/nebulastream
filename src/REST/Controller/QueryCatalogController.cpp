#include "REST/Controller/QueryCatalogController.hpp"
#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <NodeEngine/QueryStatistics.hpp>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryCatalogController::QueryCatalogController(QueryCatalogPtr queryCatalog, NesCoordinatorPtr coordinator) {
    NES_DEBUG("QueryCatalogController()");
    this->queryCatalog = queryCatalog;
    this->coordinator = coordinator;
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
                    std::map<uint64_t, std::string> queries = queryCatalog->getQueriesWithStatus(queryStatus);

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
            .then([this, message](const utility::string_t&) {
                try {
                    //Prepare the response
                    json::value result{};
                    std::map<uint64_t, std::string> queries = queryCatalog->getAllQueries();

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
    } else if (path[1] == "getNumberOfProducedBuffers") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("getNumberOfProducedBuffers called");
                    //Prepare Input query from user string
                    std::string queryId(body.begin(), body.end());
                    NES_DEBUG("getNumberOfProducedBuffers payload=" << queryId);

                    //Prepare the response
                    json::value result{};
                    size_t processedBuffers = coordinator->getQueryStatistics(std::stoi(queryId))->getProcessedBuffers();
                    NES_DEBUG("getNumberOfProducedBuffers processedBuffers=" << processedBuffers);

                    result["producedBuffers"] = processedBuffers;

                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -getNumberOfProducedBuffers: Exception occurred while fetching the number of buffers:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else if (path[1] == "status") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("Get current status of the query");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("status payload=" << payload);

                    //Prepare the response
                    json::value result{};
                    const QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(std::stoi(payload));
                    std::string currentQueryStatus = queryCatalogEntry->getQueryStatusAsString();
                    NES_DEBUG("Current query status=" << currentQueryStatus);

                    result["status"] = json::value::string(currentQueryStatus);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("QueryCatalogController: handleGet -status: Exception occurred while fetching the query status:"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

}// namespace NES
