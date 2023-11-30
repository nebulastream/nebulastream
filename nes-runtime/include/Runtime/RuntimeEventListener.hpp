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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEEVENTLISTENER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEEVENTLISTENER_HPP_
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <memory>

namespace NES::Network {
class ExchangeProtocol;
}

namespace NES::Runtime {
class BaseEvent;
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

#endif  // NES_RUNTIME_INCLUDE_RUNTIME_RUNTIMEEVENTLISTENER_HPP_
