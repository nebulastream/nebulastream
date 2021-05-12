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

#include <NodeEngine/Transactional/TransactionId.hpp>
namespace NES::NodeEngine::Transactional {

TransactionId::TransactionId(uint64_t counter): id(counter) {}
TransactionId::TransactionId(uint64_t counter, uint64_t originId, uint64_t threadId): id(counter), originId(originId), threadId(threadId) {}

bool TransactionId::operator<(const TransactionId& other) const { return this->id < other.id; }

bool TransactionId::operator>(const TransactionId& other) const { return this->id > other.id; }

}// namespace NES::NodeEngine::Transactional