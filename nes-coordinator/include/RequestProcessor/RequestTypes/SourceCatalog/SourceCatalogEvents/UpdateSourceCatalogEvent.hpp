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
#ifndef UPDATESOURCECATALOGEVENT_HPP
#define UPDATESOURCECATALOGEVENT_HPP
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>

namespace NES::RequestProcessor {

struct SourceCatalogResponse : AbstractRequestResponse {
    explicit SourceCatalogResponse(bool success) : success(success){};
    bool success;
};

class UpdateSourceCatalogEvent;
using UpdateSourceCatalogEventPtr = std::shared_ptr<UpdateSourceCatalogEvent>;

/**
* @brief This class represent a event that can be passed to a request to modify the source catalog
*/
class UpdateSourceCatalogEvent : public std::enable_shared_from_this<UpdateSourceCatalogEvent> {
  public:
    /**
     * @brief checks if the event is an instance of a specific subclass
     */
    template<class SourceCatalogEventType>
    bool insteanceOf() {
        return dynamic_cast<const SourceCatalogEventType*>(this) != nullptr;
    }

    /**
     * @brief casts the event to a specific subclass
     */
    template<class SourceCatalotEventType>
    std::shared_ptr<SourceCatalotEventType> as() {
        if (insteanceOf<SourceCatalotEventType>()) {
            return std::static_pointer_cast<SourceCatalotEventType>(shared_from_this());
        }
        std::string className = typeid(SourceCatalotEventType).name();
        throw std::logic_error("Invalid cast to " + className + " from " + typeid(*this).name());
    }

    virtual ~UpdateSourceCatalogEvent() = default;
};

}// namespace NES::RequestProcessor

#endif//UPDATESOURCECATALOGEVENT_HPP
