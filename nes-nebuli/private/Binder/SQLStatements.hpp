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

#include <Identifiers/Identifiers.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <NebuLI.hpp>

namespace NES
{

enum class SQLStatements
{
    STOP,
    STATUS,
    REGISTER,
    UNREGISTER,
    START
};

struct QueryParseResult
{
    SQLStatements type;
    std::shared_ptr<NES::QueryPlan> queryPlan;
    std::optional<QueryId> queryId;
};


struct StartQueryStatement
{
    std::shared_ptr<QueryPlan> queryPlan;
};

struct DropQueryStatement
{
    QueryId queryID;
};

struct ShowQueryStatusStatement
{
    QueryId queryID;
};

struct CreateSourceStatement
{
    std::string sourceName{};
    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> columns;
    //to enable CREATE SOURCE AS add
    //std::optional<QueryPlan> sourceQuery;
};

struct DropSourceStatement
{
    std::variant<std::shared_ptr<CLI::LogicalSource>, std::shared_ptr<CLI::PhysicalSource>> source;
};


using Statement = std::variant<CreateSourceStatement, DropSourceStatement, StartQueryStatement, DropQueryStatement>;
}
