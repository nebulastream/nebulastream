#include "REST/Controller/QueryCatalogController.hpp"

namespace NES {

void QueryCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {
    if (path[1] == "allQueries") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());

                        json::value req = json::value::parse(payload);

                        std::string queryStatus = req.at("status").as_string();

                        //Prepare the response
                        json::value result{};
//                        if (allPhysicalStream.empty()) {
//                            result = json::value::string("No Physical Stream Found.");
//                        } else {
//                            vector<json::value> allStream = {};
//                            for (auto const& physicalStream : std::as_const(allPhysicalStream)) {
//                                allStream.push_back(json::value::string(physicalStream->toString()));
//                            }
//                            result["Physical Streams"] = json::value::array(allStream);
//                        }
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
