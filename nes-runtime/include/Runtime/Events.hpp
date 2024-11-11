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
#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EVENTS_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EVENTS_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
namespace NES::Network {
class ExchangeProtocol;
}
namespace NES::Runtime {
/// this enum defines the event that can occur in the system runtime
enum class EventType : uint8_t { kInvalidEvent, kCustomEvent, kStartSourceEvent };

template<typename T>
concept IsNesEvent = requires(T t) { t.getEventType(); };

/// Design rationale: create an own event that inherits from BaseEvent for internal system events (e.g., the checkpoint barrier, the upstream ACK).
/// Use the custom event for user-specific events, e.g., feedback loops for toggling source sampling frequency.

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
 * @brief This class represents a start method for static data sources
 */
class StartSourceEvent : public BaseEvent {
  public:
    /**
     * @brief creates a custom events that lets static data sources start sending data.
     */
    explicit StartSourceEvent() : BaseEvent(EventType::kStartSourceEvent) {}

    // todo only for compliance, don't call!
    uint8_t* data() override { return nullptr; }
};

struct PropagateEpochEvent {
 explicit PropagateEpochEvent(Runtime::EventType type, uint64_t timestamp) : type(type), timestamp(timestamp) {}

 Runtime::EventType getEventType() const { return type; }

 uint64_t timestampValue() const { return timestamp; }

 Runtime::EventType type;
 uint64_t timestamp;
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EVENTS_HPP_
