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

#ifndef NES_MIGRATIONTYPE_HPP
#define NES_MIGRATIONTYPE_HPP
#include <vector>
#include <cstdint>
#include <string>

namespace NES::Experimental {

/**
 * @brief this class represent Migration Types
 */
class MigrationType {

  public:
    /**
    * RESTART means the entire query is first undeployed and then redeployed, ie restarted
    * Query is undeployed by the QueryUndeploymentPhase. The Query is then redeployed just like a new query would be.
    *
    * MIGRATION_WITH_BUFFERING means data is buffered on upstream nodes of the node marked for maintenance before migration
    * Step 1: Data is buffered on the upstream node(s) of the node marked for maintenance
    * Step 2: Query Sub Plans on the node marked for maintenance are migrated to a suitable alternative node
    *         Suitable: reachable by all upstream nodes and can reach all downstream nodes of a query sub plan
    * Step 3: upstream node(s) are reconfigured to send data to the suitable alternative node(s)
    *
    * MIGRATION_WITHOUT_BUFFERING means data is processed on the node marked for maintenance until migration is complete
    * Step 1: Query Sub Plans on the node marked for maintenance are migrated to a suitable alternative node
    *         Suitable: reachable by all upstream nodes and can reach all downstream nodes of a query sub plan
    * Step 2: upstream node(s) are reconfigured to send data to the suitable alternative node(s)
    */
    enum Value : uint8_t { INVALID = 0, RESTART = 1, MIGRATION_WITH_BUFFERING = 2, MIGRATION_WITHOUT_BUFFERING = 3 };

    /**
     *
     * @param migrationType
     * @return true if migrationType is valid, else false
     */
    static bool isValidMigrationType(Value migrationType);

    static Value getFromString(const std::string migrationType);

    static std::string toString(MigrationType::Value migrationType);
};

} //namespace NES::Experimental
#endif//NES_MIGRATIONTYPE_HPP
