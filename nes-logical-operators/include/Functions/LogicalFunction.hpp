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

#include <stack>
#include <string>
#include <span>
#include <ostream>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

/// @brief this indicates an function, which is a parameter for a FilterOperator or a MapOperator.
/// Each function declares a stamp, which expresses the data type of this function.
/// A stamp can be of a concrete type or invalid if the data type was not yet inferred.
class LogicalFunction : public std::enable_shared_from_this<LogicalFunction>
{
public:
    virtual ~LogicalFunction() = default;
    [[nodiscard]] std::string getType() const;
    virtual SerializableFunction serialize() const = 0;
    virtual bool operator==(const LogicalFunction& rhs) const = 0;
    virtual std::unique_ptr<LogicalFunction> clone() const = 0;
    virtual std::string toString() const  = 0;

    template <class FunctionType>
    std::vector<std::reference_wrapper<const FunctionType>> getFunctionByType() const
    {
        std::vector<std::reference_wrapper<const FunctionType>> results;
        std::stack<const LogicalFunction*> toVisit;
        toVisit.push(this);

        while (!toVisit.empty()) {
            const LogicalFunction* node = toVisit.top();
            toVisit.pop();
            if (auto casted = dynamic_cast<const FunctionType*>(node))
            {
                results.push_back(*casted);
            }
            for (const auto& child : node->getChildren())
            {
                toVisit.push(child.get());
            }
        }
        return results;
    }

    /// non-owning view
    const DataType& getStamp() const;
    void setStamp(std::unique_ptr<DataType> stamp);
    /// Infers the stamp if it depends on the schema. Default impl. calls inferStamp on all children
    virtual void inferStamp(const Schema& schema);
    /// non-owning view
    virtual std::span<const std::unique_ptr<LogicalFunction>> getChildren() const = 0;

protected:
    explicit LogicalFunction() = default;
    LogicalFunction(const LogicalFunction& other);

    std::unique_ptr<DataType> stamp;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalFunction& function)
{
    return os << function.toString();
}
}

FMT_OSTREAM(NES::LogicalFunction);
