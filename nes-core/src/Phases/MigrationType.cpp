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

#include <Phases/MigrationType.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Experimental {

bool MigrationType::isValidMigrationType(Value migrationType) {
    switch (migrationType) {
        case MigrationType::RESTART: return true;
        case MigrationType::MIGRATION_WITH_BUFFERING: return true;
        case MigrationType::MIGRATION_WITHOUT_BUFFERING: return true;
        default:  return false;
    }
}
std::string MigrationType::toString(MigrationType::Value migrationType) {
    switch (migrationType) {
        case MigrationType::RESTART: return "restart";
        case MigrationType::MIGRATION_WITH_BUFFERING: return "migration with buffering";
        case MigrationType::MIGRATION_WITHOUT_BUFFERING: return "migration without buffering";
        default: return "Invalid";
    }
}

MigrationType::Value MigrationType::getFromString(const std::string migrationType) {
    if (migrationType == "restart") {
        return MigrationType::RESTART;
    } else if (migrationType == "migration with buffering") {
        return MigrationType::MIGRATION_WITH_BUFFERING;
    }   else if (migrationType == "migration without buffering") {
        return MigrationType::MIGRATION_WITHOUT_BUFFERING;
    } else {
        return MigrationType::INVALID;
    }
}
}//namespace NES::Experimental
