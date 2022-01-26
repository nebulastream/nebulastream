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
#ifndef NES_INCLUDE_RUNTIME_ERRORLISTENER_HPP_
#define NES_INCLUDE_RUNTIME_ERRORLISTENER_HPP_

#include <Util/VirtualEnableSharedFromThis.hpp>
#include <memory>

namespace NES {

/**
 * @brief This is an error interceptor suitable for signals and exceptions
 */
class ErrorListener : public detail::virtual_enable_shared_from_this<ErrorListener> {
  public:
    /**
     * @brief onFatalError shall be called when a signal is caught
     * @param signalNumber the caught signal
     * @param stacktrace the stacktrace of the error
     */
    virtual void onFatalError(int signalNumber, std::string) = 0;

    /**
     * @brief onFatalException shall be called when an exception is caught
     * @param exception the caught exception
     * @param stacktrace the stacktrace of the error
     */
    virtual void onFatalException(std::shared_ptr<std::exception>, std::string) = 0;
};

}// namespace NES

#endif  // NES_INCLUDE_RUNTIME_ERRORLISTENER_HPP_
