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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEGETSOURCECATALOGEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEGETSOURCECATALOGEVENT_HPP_

#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <nlohmann/json.hpp>

namespace NES::RequestProcessor {
struct BaseGetSourceCatalogResponse : AbstractRequestResponse {
    explicit BaseGetSourceCatalogResponse(bool success) : success(success){};
    bool success;
};

/**
 * @brief a response containing source information as json
 */
struct BaseGetSourceJsonResponse : public BaseGetSourceCatalogResponse {
    explicit BaseGetSourceJsonResponse(bool success, nlohmann::json json) : BaseGetSourceCatalogResponse(success), json(json){};
    nlohmann::json getJson();

  private:
    nlohmann::json json;
};

/**
* @brief This class represents a event that can be passed to a request to get information about logical or physical sources
*/
class BaseGetSourceCatalogEvent : public std::enable_shared_from_this<BaseGetSourceCatalogEvent> {
  public:
    /**
     * @brief checks if the event is an instance of a specific subclass
     */
    template<class BaseGetSourceCatalogEventType>
    bool instanceOf() {
        return dynamic_cast<const BaseGetSourceCatalogEventType*>(this) != nullptr;
    }

    /**
     * @brief casts the event to a specific subclass
     */
    template<class BaseGetSourceCatalogEventType>
    std::shared_ptr<BaseGetSourceCatalogEventType> as() {
        if (instanceOf<BaseGetSourceCatalogEventType>()) {
            return std::static_pointer_cast<BaseGetSourceCatalogEventType>(shared_from_this());
        }
        std::string className = typeid(BaseGetSourceCatalogEventType).name();
        throw std::logic_error("Invalid cast to " + className + " from " + typeid(*this).name());
    }

    virtual ~BaseGetSourceCatalogEvent() = default;
};

}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEGETSOURCECATALOGEVENT_HPP_
