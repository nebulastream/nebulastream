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

#include <Util/magicenum/magic_enum.hpp>
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>

NES::Experimental::MigrateQueryRequestPtr
NES::Experimental::MigrateQueryRequest::create(NES::QueryId queryId, NES::Experimental::MigrationType migrationType) {
    return std::make_shared<MigrateQueryRequest>(MigrateQueryRequest(queryId, migrationType));
}

NES::Experimental::MigrateQueryRequest::MigrateQueryRequest(NES::QueryId queryId, NES::Experimental::MigrationType migrationType)
    : queryId(queryId), migrationType(migrationType) {}

std::string NES::Experimental::MigrateQueryRequest::toString() {
    return "QueryMigrationRequest { Query Id: " + std::to_string(queryId)
        + ", withBuffer: " + std::string(magic_enum::enum_name(migrationType)) + "}";
}

NES::Experimental::MigrationType NES::Experimental::MigrateQueryRequest::getMigrationType() { return migrationType; }

NES::QueryId NES::Experimental::MigrateQueryRequest::getQueryId() { return queryId; }
