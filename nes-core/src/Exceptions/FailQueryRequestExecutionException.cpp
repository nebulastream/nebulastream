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
#include <Exceptions/FailQueryRequestExecutionException.h>

namespace NES {
//todo: add error handling to force supplying sharedQueryId dependeing on stage
FailQueryRequestExecutionException::FailQueryRequestExecutionException(uint8_t stage)
    : CoordinatorRequestExecutionException(stage) {}

FailQueryRequestExecutionException::FailQueryRequestExecutionException(uint8_t stage, uint64_t sharedQueryId)
    : CoordinatorRequestExecutionException(stage), sharedQueryId(sharedQueryId) {}

uint64_t FailQueryRequestExecutionException::getSharedQueryid() { return sharedQueryId; }
}// namespace NES