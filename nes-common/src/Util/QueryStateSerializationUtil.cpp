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

#include <Util/Logger/Logger.hpp>
#include <Util/QueryStateSerializationUtil.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

SerializableQueryState QueryStateSerializationUtil::serializeQueryState(QueryState queryState) {
    using enum QueryState;
    switch (queryState) {
        case REGISTERED: return QUERY_STATE_REGISTERED;
        case OPTIMIZING: return QUERY_STATE_OPTIMIZING;
        case DEPLOYED: return QUERY_STATE_DEPLOYED;
        case RUNNING: return QUERY_STATE_RUNNING;
        case MARKED_FOR_HARD_STOP: return QUERY_STATE_MARKED_FOR_HARD_STOP;
        case MARKED_FOR_SOFT_STOP: return QUERY_STATE_MARKED_FOR_SOFT_STOP;
        case SOFT_STOP_TRIGGERED: return QUERY_STATE_SOFT_STOP_TRIGGERED;
        case SOFT_STOP_COMPLETED: return QUERY_STATE_SOFT_STOP_COMPLETED;
        case STOPPED: return QUERY_STATE_STOPPED;
        case MARKED_FOR_FAILURE: return QUERY_STATE_MARKED_FOR_FAILURE;
        case FAILED: return QUERY_STATE_FAILED;
        case RESTARTING: return QUERY_STATE_RESTARTING;
        case MIGRATING: return QUERY_STATE_MIGRATING;
        case MIGRATION_COMPLETED: return QUERY_STATE_MIGRATION_COMPLETED;
        case EXPLAINED: return QUERY_STATE_EXPLAINED;
        case REDEPLOYED: return QUERY_STATE_REDEPLOYED;
        case MARKED_FOR_DEPLOYMENT: return QUERY_STATE_MARKED_FOR_DEPLOYMENT;
        case MARKED_FOR_REDEPLOYMENT: return QUERY_STATE_MARKED_FOR_REDEPLOYMENT;
        case MARKED_FOR_MIGRATION: return QUERY_STATE_MARKED_FOR_MIGRATION;
        case MARKED_FOR_UPDATE_AND_DRAIN: return QUERY_STATE_MARKED_FOR_UPDATE_AND_DRAIN;
    }
}

QueryState QueryStateSerializationUtil::deserializeQueryState(SerializableQueryState serializedQueryState) {
    using enum QueryState;
    switch (serializedQueryState) {
        case QUERY_STATE_REGISTERED: return REGISTERED;
        case QUERY_STATE_OPTIMIZING: return OPTIMIZING;
        case QUERY_STATE_DEPLOYED: return DEPLOYED;
        case QUERY_STATE_RUNNING: return RUNNING;
        case QUERY_STATE_MARKED_FOR_HARD_STOP: return MARKED_FOR_HARD_STOP;
        case QUERY_STATE_MARKED_FOR_SOFT_STOP: return MARKED_FOR_SOFT_STOP;
        case QUERY_STATE_SOFT_STOP_TRIGGERED: return SOFT_STOP_TRIGGERED;
        case QUERY_STATE_SOFT_STOP_COMPLETED: return SOFT_STOP_COMPLETED;
        case QUERY_STATE_STOPPED: return STOPPED;
        case QUERY_STATE_MARKED_FOR_FAILURE: return MARKED_FOR_FAILURE;
        case QUERY_STATE_FAILED: return FAILED;
        case QUERY_STATE_RESTARTING: return RESTARTING;
        case QUERY_STATE_MIGRATING: return MIGRATING;
        case QUERY_STATE_MIGRATION_COMPLETED: return MIGRATION_COMPLETED;
        case QUERY_STATE_EXPLAINED: return EXPLAINED;
        case QUERY_STATE_REDEPLOYED: return REDEPLOYED;
        case QUERY_STATE_MARKED_FOR_DEPLOYMENT: return MARKED_FOR_DEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_REDEPLOYMENT: return MARKED_FOR_REDEPLOYMENT;
        case QUERY_STATE_MARKED_FOR_MIGRATION: return MARKED_FOR_MIGRATION;
        case QUERY_STATE_MARKED_FOR_UPDATE_AND_DRAIN: return MARKED_FOR_UPDATE_AND_DRAIN;
        case SerializableQueryState_INT_MIN_SENTINEL_DO_NOT_USE_:
        case SerializableQueryState_INT_MAX_SENTINEL_DO_NOT_USE_: {
            NES_WARNING("Warning invalid query state {}. Returning registered as default query state.",
                        magic_enum::enum_name(serializedQueryState));
            return REGISTERED;
        }
    }
}

}// namespace NES
