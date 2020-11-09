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

#ifndef NES_QUERYPLANSERIALIZATIONUTIL_HPP
#define NES_QUERYPLANSERIALIZATIONUTIL_HPP

#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SerializableQueryPlan;

class QueryPlanSerializationUtil {
  public:
    /**
     * @brief Serializes a Query Plan and all its root operators to a SerializableQueryPlan object.
     * @param queryPlan: The query plan
     * @return the pointer to serialized SerializableQueryPlan
     */
    static SerializableQueryPlan* serializeQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief De-serializes the SerializableQueryPlan and all its root operators back to a QueryPlanPtr
     * @param serializedQueryPlan the serialized query plan.
     * @return the pointer to the deserialized query plan
     */
    static QueryPlanPtr deserializeQueryPlan(SerializableQueryPlan* serializedQueryPlan);
};
}// namespace NES
#endif//NES_QUERYPLANSERIALIZATIONUTIL_HPP
