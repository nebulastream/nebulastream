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

#ifndef NES_CORE_INCLUDE_REST_RESTSERVERINTERRUPTHANDLER_HPP_
#define NES_CORE_INCLUDE_REST_RESTSERVERINTERRUPTHANDLER_HPP_

namespace NES {

/**
 * @brief Interrupt Handler for the rest server.
 * This is used to wait for the termination of the rest server.
 */
class RestServerInterruptHandler {
  public:
    static void hookUserInterruptHandler();

    static void handleUserInterrupt(int signal);

    static void waitForUserInterrupt();
};
}// namespace NES

#endif// NES_CORE_INCLUDE_REST_RESTSERVERINTERRUPTHANDLER_HPP_
