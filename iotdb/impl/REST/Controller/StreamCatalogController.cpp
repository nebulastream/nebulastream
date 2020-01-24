#include "REST/Controller/StreamCatalogController.hpp"

using namespace web;
using namespace http;

namespace NES {

void StreamCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "allLogicalStream") {

        const map<std::string, std::string>
            & allLogicalStreamAsString = streamCatalogService.getAllLogicalStreamAsString();

        json::value result{};
        if (allLogicalStreamAsString.empty()) {
            result = json::value::string("No Logical Stream Found.");
        } else {
            for (auto const&[key, val] : allLogicalStreamAsString) {
                result[key] = json::value::string(val);
            }
        }
        successMessageImpl(message, result);
        return;

    } else if (path[1] == "allPhysicalStream") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        string logicalStreamName = req.at("streamName").as_string();

                        const vector<StreamCatalogEntryPtr>
                            & allPhysicalStream = streamCatalogService.getAllPhysicalStream(logicalStreamName);

                        //Prepare the response
                        json::value result{};
                        if (allPhysicalStream.empty()) {
                            result = json::value::string("No Physical Stream Found.");
                        } else {
                            vector<json::value> allStream = {};
                            for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
                                allStream.push_back(json::value::string(physicalStream->toString()));
                            }
                            result["Physical Streams"] = json::value::array(allStream);
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

void StreamCatalogController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "addLogicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        string streamName = req.at("streamName").as_string();
                        string schema = req.at("schema").as_string();

                        bool added = streamCatalogService.addNewLogicalStream(streamName, schema);

                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(added);
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
    } else if (path[1] == "updateLogicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        string streamName = req.at("streamName").as_string();
                        string schema = req.at("schema").as_string();

                        bool added = streamCatalogService.updatedLogicalStream(streamName, schema);

                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(added);
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

void StreamCatalogController::handleDelete(std::vector<utility::string_t> path, web::http::http_request message) {

    if (path[1] == "deleteLogicalStream") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        string streamName = req.at("streamName").as_string();

                        bool added = streamCatalogService.removeLogicalStream(streamName);

                        //Prepare the response
                        json::value result{};
                        result["Success"] = json::value::boolean(added);
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

}
