/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_RUNTIME_EVENTS_HPP_
#define NES_INCLUDE_RUNTIME_EVENTS_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
namespace NES::Network {
class ExchangeProtocol;
}
namespace NES::Runtime {
/// this enum defines the event that can occur in the system runtime
enum class EventType : uint8_t { kInvalidEvent, kCustomEvent };

template<typename T>
concept IsNesEvent = requires(T t) {
    t.getEventType();
};

/// Design rationale: create an own event that inherits from BaseEvent for internal system events (e.g., the checkpoint barrier, the upstream ACK).
/// Use the custom event for user-specific events, e.g., feedback loops for toggling source sampling fequency.

/**
 * @brief This is the base event type. All events supported in NES shall inherit from this class
 */
class BaseEvent {
  public:
    /**
     * @brief Creates an event of a given type
     * @param eventType
     */
    explicit BaseEvent(EventType eventType = EventType::kInvalidEvent) : eventType(eventType) {}

    /**
     * @brief Gets the payload of the event
     * @return the payload of the event
     */
    virtual uint8_t* data() = 0;

    /**
     * @brief The event type
     * @return
     */
    EventType getEventType() const { return eventType; }

  private:
    EventType eventType;
};

/**
 * @brief This class shall be used to define custom events with user-supplied data
 */
class CustomEventWrapper : public BaseEvent {
  public:
    /**
     * @brief creates a custom events that stores a buffer as a payload
     * @param buffer
     */
    explicit CustomEventWrapper(Runtime::TupleBuffer&& buffer) : BaseEvent(EventType::kCustomEvent), buffer(buffer) {}

    uint8_t* data() override { return buffer.getBuffer(); }

    template<typename T>
    T* data() {
        return buffer.getBuffer<T>();
    }

  private:
    Runtime::TupleBuffer buffer;
};

/**
 * @brief This is the listener for runtime events
 */
class RuntimeEventListener : public NES::detail::virtual_enable_shared_from_this<RuntimeEventListener, false> {
    friend class NES::Network::ExchangeProtocol;

  protected:
    /**
     * @brief API method called upon receiving an event
     * @param event
     */
    virtual void onEvent(Runtime::BaseEvent& event) = 0;
};
using RuntimeEventListenerPtr = std::shared_ptr<RuntimeEventListener>;

}// namespace NES::Runtime

#endif  // NES_INCLUDE_RUNTIME_EVENTS_HPP_
