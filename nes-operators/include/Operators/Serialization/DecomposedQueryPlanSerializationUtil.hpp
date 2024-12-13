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
#include <Util/QueryState.hpp>

namespace NES
{
enum SerializableQueryState : int;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

class SerializableDecomposedQueryPlan;

class DecomposedQueryPlanSerializationUtil
{
public:
    /**
     * @brief Serializes a Query Plan and all its root operators to a SerializableQueryPlan object.
     * @param queryPlan: The query plan
     * @param serializableDecomposedQueryPlan: pointer to the serializable decomposed query Plan object
     * @return the pointer to serialized SerializableQueryPlan
     */
    static void serializeDecomposedQueryPlan(
        const DecomposedQueryPlan& decomposedQueryPlan, SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan);

    /**
     * @brief De-serializes the SerializableQueryPlan and all its root operators back to a QueryPlanPtr
     * @param serializableDecomposedQueryPlan the serialized decomposed query plan.
     * @return the pointer to the deserialized query plan
     */
    static DecomposedQueryPlanPtr deserializeDecomposedQueryPlan(const SerializableDecomposedQueryPlan* serializableDecomposedQueryPlan);

    static NES::QueryState deserializeQueryState(NES::SerializableQueryState serializedQueryState);

    static SerializableQueryState serializeQueryState(QueryState queryState);
};
}
