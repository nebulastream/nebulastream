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
#include <API/Schema.hpp>
#include <Nodes/Node.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class FunctionNode;
using FunctionNodePtr = std::shared_ptr<FunctionNode>;

/**
 * @brief this indicates an function, which is a parameter for a FilterOperator or a MapOperator.
 * Each function declares a stamp, which expresses the data type of this function.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class FunctionNode : public Node
{
public:
    explicit FunctionNode(DataTypePtr stamp);

    ~FunctionNode() override = default;

    /**
     * @brief Indicates if this function is a predicate -> if its result stamp is a boolean
     * @return
     */
    bool isPredicate() const;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    virtual void inferStamp(SchemaPtr schema);

    /**
     * @brief returns the stamp as the data type which is produced by this function.
     * @return Stamp
     */
    DataTypePtr getStamp() const;

    /**
     * @brief sets the stamp of this function.
     * @param stamp
     */
    void setStamp(DataTypePtr stamp);

    /**
     * @brief Create a deep copy of this function node.
     * @return FunctionNodePtr
     */
    virtual FunctionNodePtr copy() = 0;

protected:
    explicit FunctionNode(const FunctionNode* other);

    /**
     * @brief declares the type of this function.
     * todo replace the direct usage of data types with a stamp abstraction.
     */
    DataTypePtr stamp;
};
} /// namespace NES
