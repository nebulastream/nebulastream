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

#ifndef NES_ISQPEVENTPRIORITY_HPP
#define NES_ISQPEVENTPRIORITY_HPP

#include <cstdint>

namespace NES::RequestProcessor {
// smallest number has the highest priority
constexpr uint8_t ISQP_ADD_NODE_EVENT_PRIORITY = 0;
constexpr uint8_t ISQP_ADD_LINK_EVENT_PRIORITY = 1;
constexpr uint8_t ISQP_REMOVE_LINK_EVENT_PRIORITY = 2;
constexpr uint8_t ISQP_REMOVE_NODE_EVENT_PRIORITY = 3;
constexpr uint8_t ISQP_ADD_QUERY_EVENT_PRIORITY = 4;
constexpr uint8_t ISQP_REMOVE_QUERY_EVENT_PRIORITY = 5;
}// namespace NES::RequestProcessor
#endif//NES_ISQPEVENTPRIORITY_HPP
