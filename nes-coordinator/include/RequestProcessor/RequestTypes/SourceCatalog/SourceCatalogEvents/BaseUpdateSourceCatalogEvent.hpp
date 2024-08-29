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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEUPDATESOURCECATALOGEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEUPDATESOURCECATALOGEVENT_HPP_

#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>

namespace NES::RequestProcessor {
struct BaseUpdateSourceCatalogResponse : AbstractRequestResponse {
    explicit BaseUpdateSourceCatalogResponse(bool success) : success(success){};
    bool success;
};

class BaseUpdateSourceCatalogEvent;
using UpdateSourceCatalogEventPtr = std::shared_ptr<BaseUpdateSourceCatalogEvent>;

/**
* @brief This class represent a event that can be passed to a request to modify the source catalog
*/
class BaseUpdateSourceCatalogEvent : public std::enable_shared_from_this<BaseUpdateSourceCatalogEvent> {
  public:
    /**
     * @brief checks if the event is an instance of a specific subclass
     */
    template<class BaseUpdateSourceCatalogEventType>
    bool instanceOf() {
        return dynamic_cast<const BaseUpdateSourceCatalogEventType*>(this) != nullptr;
    }

    /**
     * @brief casts the event to a specific subclass
     */
    template<class BaseUpdateSourceCatalogEventType>
    std::shared_ptr<BaseUpdateSourceCatalogEventType> as() {
        if (instanceOf<BaseUpdateSourceCatalogEventType>()) {
            return std::static_pointer_cast<BaseUpdateSourceCatalogEventType>(shared_from_this());
        }
        std::string className = typeid(BaseUpdateSourceCatalogEventType).name();
        throw std::logic_error("Invalid cast to " + className + " from " + typeid(*this).name());
    }

    virtual ~BaseUpdateSourceCatalogEvent() = default;
};

}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_BASEUPDATESOURCECATALOGEVENT_HPP_
