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
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class NodeFunction;
using NodeFunctionPtr = std::shared_ptr<NodeFunction>;

/**
 * @brief this indicates an function, which is a parameter for a FilterOperator or a MapOperator.
 * Each function declares a stamp, which expresses the data type of this function.
 * A stamp can be of a concrete type or invalid if the data type was not yet inferred.
 */
class NodeFunction : public Node
{
public:
    explicit NodeFunction(DataTypePtr stamp, std::string type);

    ~NodeFunction() override = default;

    bool isPredicate() const;

    ///Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema);

    DataTypePtr getStamp() const;
    void setStamp(DataTypePtr stamp);
    [[nodiscard]] const std::string& getType() const;

    /// Checks if the function is valid. This means that the function can be lowered to an executable function.
    /// This entails checking for configuration mismatches and other issues.
    virtual bool validateBeforeLowering() const = 0;

    /// Create a deep copy of this function node.
    virtual NodeFunctionPtr deepCopy() = 0;

protected:
    explicit NodeFunction(const NodeFunction* other);

    /**
     * @brief declares the type of this function.
     * todo replace the direct usage of data types with a stamp abstraction.
     */
    DataTypePtr stamp;
    const std::string type;
};
}
