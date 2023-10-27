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

#include <Runtime/ReconfigurationMessage.hpp>

namespace NES::Runtime {

void ReconfigurationMessage::wait() { syncBarrier->wait(); }

void ReconfigurationMessage::postWait() {
    if (postSyncBarrier != nullptr) {
        postSyncBarrier->wait();
    }
}

void ReconfigurationMessage::postReconfiguration() {
    //if ref count gets 0, we know all threads did the reconfiguration
    if (refCnt.fetch_sub(1) == 1) {
        instance->postReconfigurationCallback(*this);
    }
}

void ReconfigurationMessage::destroy() {
    syncBarrier.reset();
    postSyncBarrier.reset();
}

}// namespace NES::Runtime