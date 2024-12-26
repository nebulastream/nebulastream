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

#include "../../include/Runtime/OperatorHandlerStore.hpp"

namespace NES {

OperatorHandlerStorePtr OperatorHandlerStore::create() { return std::make_shared<OperatorHandlerStore>(); }

bool OperatorHandlerStore::contains(const SharedQueryId queryId, const DecomposedQueryId planId, const OperatorId operatorId) {
    return (*operatorHandlerStorage.rlock()).contains(OperatorHash(queryId, planId, operatorId));
}

Runtime::Execution::OperatorHandlerPtr OperatorHandlerStore::getOperatorHandler(const SharedQueryId queryId,
                                                                                const DecomposedQueryId planId,
                                                                                const OperatorId operatorId) {
    auto lockedStorage = operatorHandlerStorage.rlock();
    return (*lockedStorage).at(OperatorHash(queryId, planId, operatorId));
}

void OperatorHandlerStore::storeOperatorHandler(const SharedQueryId queryId,
                                                const DecomposedQueryId planId,
                                                const OperatorId operatorId,
                                                Runtime::Execution::OperatorHandlerPtr handler) {
    auto lockedStorage = operatorHandlerStorage.wlock();
    (*lockedStorage)[OperatorHash(queryId, planId, operatorId)] = handler;
}

void OperatorHandlerStore::removeOperatorHandlers(const SharedQueryId queryId) {
    auto lockedStorage = operatorHandlerStorage.wlock();
    erase_if((*lockedStorage), [queryId](const auto pair) {
        return get<0>(pair.first) == queryId;
    });
}

void OperatorHandlerStore::removeOperatorHandlers(const SharedQueryId queryId, const DecomposedQueryId planId) {
    auto lockedStorage = operatorHandlerStorage.wlock();
    erase_if((*lockedStorage), [queryId, planId](const auto pair) {
        return get<0>(pair.first) == queryId && get<1>(pair.first) == planId;
    });
}
}// namespace NES
