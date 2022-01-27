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

#ifndef NES_MIGRATIONTYPES_HPP
#define NES_MIGRATIONTYPES_HPP
#include <vector>
#include <cstdint>

namespace NES::Experimental {

/**
 * @brief these alias represent Migration Types
 */
using MigrationType = uint8_t;
/**
 * @brief RESTART means the entire query is first undeployed and then redeployed, ie restarted
 *
 * Query is undeployed by the QueryUndeploymentPhase. The Query is then redeployed just like a new query would be.
 */
static constexpr uint8_t RESTART = 1;
/**
 * @brief MIGRATION_WITH_BUFFERING means data is buffered on upstream nodes of the node marked for maintenance before migration
 *
 * Step 1: Data is buffered on the upstream node(s) of the node marked for maintenance
 * Step 2: Query Sub Plans on the node marked for maintenance are migrated to a suitable alternative node
 *         Suitable: reachable by all upstream nodes and can reach all downstream nodes of a query sub plan
 * Step 3: upstream node(s) are reconfigured to send data to the suitable alternative node(s)
 */
static constexpr uint8_t MIGRATION_WITH_BUFFERING = 2;
/**
 * @brief MIGRATION_WITHOUT_BUFFERING means data is processed on the node marked for maintenance until migration is complete
 *
 * Step 1: Query Sub Plans on the node marked for maintenance are migrated to a suitable alternative node
 *         Suitable: reachable by all upstream nodes and can reach all downstream nodes of a query sub plan
 * Step 2: upstream node(s) are reconfigured to send data to the suitable alternative node(s)
 */
static constexpr uint8_t MIGRATION_WITHOUT_BUFFERING = 3;
inline static const std::vector<MigrationType> migrationTypes = {RESTART, MIGRATION_WITH_BUFFERING, MIGRATION_WITHOUT_BUFFERING};
}
#endif//NES_MIGRATIONTYPES_HPP
