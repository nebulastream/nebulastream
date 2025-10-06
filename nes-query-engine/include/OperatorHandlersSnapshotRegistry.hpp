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

#include <mutex>
#include <unordered_map>
#include <vector>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES {

// Lightweight registry to expose currently running operator handlers per query.
// Populated when pipelines start; read by QueryEngine::snapshotOperatorHandlers.
class OperatorHandlersSnapshotRegistry {
public:
    using Entry = std::pair<OperatorHandlerId, std::shared_ptr<OperatorHandler>>;

    static void add(QueryId qid, const std::vector<Entry>& entries) {
        std::scoped_lock lk(mtx());
        auto& map = registry();
        auto& bucket = map[qid.getRawValue()];
        for (const auto& e : entries) {
            bucket[e.first] = e.second;
        }
    }

    static std::vector<Entry> get(QueryId qid) {
        std::scoped_lock lk(mtx());
        std::vector<Entry> out;
        auto it = registry().find(qid.getRawValue());
        if (it == registry().end()) return out;
        out.reserve(it->second.size());
        for (auto& kv : it->second) out.emplace_back(kv.first, kv.second);
        return out;
    }

    static void clear(QueryId qid) {
        std::scoped_lock lk(mtx());
        registry().erase(qid.getRawValue());
    }

private:
    using Bucket = std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>;
    using Store = std::unordered_map<QueryId::Underlying, Bucket>;

    static std::mutex& mtx() {
        static std::mutex m; return m;
    }
    static Store& registry() {
        static Store s; return s;
    }
};

}
