/*
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
#ifndef GETSOURCECATALOGEVENT_HPP
#define GETSOURCECATALOGEVENT_HPP
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <nlohmann/json.hpp>
namespace NES::RequestProcessor {

struct GetSourceCatalogResponse : AbstractRequestResponse {
    explicit GetSourceCatalogResponse(bool success) : success(success){};
    bool success;
};

// a response containing source inforamtion as json
struct GetSourceJsonResponse : public GetSourceCatalogResponse {
    explicit GetSourceJsonResponse(bool success, nlohmann::json json) : GetSourceCatalogResponse(success), json(json){};
    nlohmann::json getJson();

  private:
    nlohmann::json json;
};

class GetSourceCatalogEvent : public std::enable_shared_from_this<GetSourceCatalogEvent> {
  public:
    /**
     * @brief checks if the event is an instance of a specific subclass
     */
    template<class GetSourceInfromationEventType>
    bool insteanceOf() {
        return dynamic_cast<const GetSourceInfromationEventType*>(this) != nullptr;
    }

    /**
     * @brief casts the event to a specific subclass
     */
    template<class GetSourceInfromationEventType>
    std::shared_ptr<GetSourceInfromationEventType> as() {
        if (insteanceOf<GetSourceInfromationEventType>()) {
            return std::static_pointer_cast<GetSourceInfromationEventType>(shared_from_this());
        }
        std::string className = typeid(GetSourceInfromationEventType).name();
        throw std::logic_error("Invalid cast to " + className + " from " + typeid(*this).name());
    }

    virtual ~GetSourceCatalogEvent() = default;
};

}// namespace NES::RequestProcessor

#endif//GETSOURCECATALOGEVENT_HPP
