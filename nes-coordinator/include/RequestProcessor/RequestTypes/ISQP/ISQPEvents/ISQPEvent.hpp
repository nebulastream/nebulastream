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

#ifndef NES_ISQPEVENT_HPP
#define NES_ISQPEVENT_HPP

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEventPriority.hpp>
#include <Util/Logger/Logger.hpp>
#include <future>

namespace NES::RequestProcessor {

class ISQPEvent;
using ISQPEventPtr = std::shared_ptr<ISQPEvent>;

/**
* @brief base class of ISQP response type
*/
struct ISQPResponse {};
using ISQPResponsePtr = std::shared_ptr<ISQPResponse>;

/**
 * @brief base class for the ISQP event types
 */
class ISQPEvent : public std::enable_shared_from_this<ISQPEvent> {

  public:
    /**
     * @brief Ctor
     * @param priority: priority of the external event
     */
    explicit ISQPEvent(uint8_t priority);

    /**
     * @brief Get the response after applying the event
     */
    std::future<ISQPResponsePtr> getResponse();

    std::promise<ISQPResponsePtr> response;

    /**
     * @brief Checks if the current event is of type ISQPExternalEvent
     * @tparam ISQPExternalEvent
     * @return bool true if node is of specific event type
     */
    template<class ISQPEventType>
    bool instanceOf() {
        if (dynamic_cast<ISQPEventType*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Typecast into shared object of type ISQPExternalEvent
     * @tparam ISQPExternalEvent
     * @return shared object of specific event type
     */
    template<class ISQPEventType>
    std::shared_ptr<ISQPEventType> as() {
        if (instanceOf<ISQPEventType>()) {
            return std::dynamic_pointer_cast<ISQPEventType>(this->shared_from_this());
        }
        std::string className = typeid(ISQPEventType).name();
        throw std::logic_error("We performed an invalid cast of ISQPExternalEvent to type " + className);
    };

    uint8_t getPriority() const;

    virtual ~ISQPEvent() = default;

  private:
    uint8_t priority;
};
}// namespace NES::RequestProcessor

#endif//NES_ISQPEVENT_HPP
