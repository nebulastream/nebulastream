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
#include <Plans/QueryPlan.hpp>
#include <Util/QueryState.hpp>
#include <SerializableQueryPlan.pb.h>

namespace NES
{

class QueryPlanSerializationUtil
{
public:
    /**
     * @brief Serializes a Query Plan and all its root operators to a SerializableQueryPlan object.
     * @param queryPlan: The query plan
     * @param serializableQueryPlan: pointer to the serializable query Plan object
     * @param isClientOriginated Indicate if the source operator is originated from a client.
     * @return the pointer to serialized SerializableQueryPlan
     */
    static void
    serializeQueryPlan(const QueryPlan& queryPlan, SerializableQueryPlan* serializableQueryPlan, bool isClientOriginated = false);

    /**
     * @brief De-serializes the SerializableQueryPlan and all its root operators back to a std::shared_ptr<QueryPlan>
     * @param serializedQueryPlan the serialized query plan.
     * @return the pointer to the deserialized query plan
     */
    static std::shared_ptr<QueryPlan> deserializeQueryPlan(const SerializableQueryPlan* serializedQueryPlan);
};
}
