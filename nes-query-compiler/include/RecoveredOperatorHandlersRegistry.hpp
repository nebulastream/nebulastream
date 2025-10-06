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

#pragma once
#include <unordered_map>
#include <memory>
#include <mutex>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES {

class RecoveredOperatorHandlersRegistry {
public:
    using HandlerMap = std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>;
    static void setPending(HandlerMap map);
    static void consumePendingForQuery(QueryId qid);
    static HandlerMap getForQuery(QueryId qid);
    static void clearForQuery(QueryId qid);

private:
    static std::mutex mtx;
    static inline HandlerMap pendingHandlers{};
    static inline std::unordered_map<QueryId, HandlerMap> registry;
};

}
