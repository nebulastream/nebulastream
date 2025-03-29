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

#include <memory>
#include <vector>
#include "Operators/Operator.hpp"

namespace NES
{

// TODO: Temporary, move into query.
struct Query
{
    std::vector<std::shared_ptr<Operator>> getSources() { return {queryRoot}; }

    std::vector<std::shared_ptr<Operator>> getSinks() { return {queryRoot}; }

    std::shared_ptr<Operator> queryRoot;
};

/// TODO: We want to work on them as query network..
/// The QueryForSubtree trait stores a pointer to a subtree of the query plan, or perhaps some metadata about it.
/*struct QueryForSubtree
{
    QueryForSubtree() = default;

    explicit QueryForSubtree(std::shared_ptr<Query> query) : query(std::move(query)) { }
    std::shared_ptr<Query> getQuery() { return query; }

private:
    std::shared_ptr<Query> query;
};*/


/// Version by Leonhard
class QueryForSubtree
{
    const std::string str;

public:
    explicit QueryForSubtree(std::string str) : str(std::move(str)) { }
    bool operator==(const QueryForSubtree&) const { return true; }
    static bool atNode() { return false; }

    const std::string& getQuery() const {
        return str;
    }
};

}
