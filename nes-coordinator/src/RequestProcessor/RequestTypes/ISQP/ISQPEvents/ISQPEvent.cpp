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

#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPEvent.hpp>

namespace NES::RequestProcessor {

ISQPEvent::ISQPEvent(uint8_t priority) : priority(priority) {}

std::future<ISQPResponsePtr> ISQPEvent::getResponse() { return response.get_future(); }

uint8_t ISQPEvent::getPriority() const { return priority; }
}// namespace NES::RequestProcessor
