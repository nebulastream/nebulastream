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

#ifndef NES_QUERYRECONFIGURATIONPLANSERIALIZATIONUTIL_HPP
#define NES_QUERYRECONFIGURATIONPLANSERIALIZATIONUTIL_HPP

#include <GRPC/Serialization/QueryReconfigurationPlanSerializationUtil.hpp>
#include <memory>

namespace NES {

class QueryReconfigurationPlan;
typedef std::shared_ptr<QueryReconfigurationPlan> QueryReconfigurationPlanPtr;

class SerializableQueryReconfigurationPlan;

class QueryReconfigurationPlanSerializationUtil {
  public:
    /**
     * @brief Serializes a Query Reconfiguration Plan to a SerializableQueryReconfigurationPlan object.
     * @param queryPlan: The Reconfiguration plan
     * @return the pointer to serialized SerializableQueryReconfigurationPlan
     */
    static SerializableQueryReconfigurationPlan*
    serializeQueryReconfigurationPlan(QueryReconfigurationPlanPtr queryReconfigurationPlan);

    /**
     * @brief De-serializes the SerializableQueryReconfigurationPlan back to a QueryReconfigurationPlanPtr
     * @param serializedQueryReconfigurationPlan the serialized query reconfiguration plan.
     * @return the pointer to the deserialized query reconfiguration plan
     */
    static QueryReconfigurationPlanPtr
    deserializeQueryReconfigurationPlan(SerializableQueryReconfigurationPlan* serializedQueryReconfigurationPlan);
};
}// namespace NES
#endif//NES_QUERYRECONFIGURATIONPLANSERIALIZATIONUTIL_HPP
