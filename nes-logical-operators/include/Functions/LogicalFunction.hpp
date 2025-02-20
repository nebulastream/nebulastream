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

#include <span>
#include <ostream>
#include <API/Schema.hpp>
#include <Util/Common.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
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

    /// Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema);
    std::shared_ptr<DataType> getStamp() const;
    void setStamp(std::shared_ptr<DataType> stamp);
    [[nodiscard]] bool isPredicate();

    [[nodiscard]] std::string getType() const;

    virtual SerializableFunction serialize() const = 0;

    virtual bool operator==(std::shared_ptr<LogicalFunction> const& rhs) const = 0;
    virtual std::shared_ptr<LogicalFunction> clone() const = 0;
    virtual std::string toString() const  = 0;

    template <class FunctionType>
    std::vector<std::shared_ptr<FunctionType>> getFunctionByType() const
    {
        std::vector<std::shared_ptr<FunctionType>> results;
        std::stack<std::shared_ptr<LogicalFunction>> toVisit;
        toVisit.push(std::const_pointer_cast<LogicalFunction>(this->shared_from_this()));

        while (!toVisit.empty()) {
            auto node = toVisit.top();
            toVisit.pop();
            if (auto casted = std::dynamic_pointer_cast<FunctionType>(node))
            {
                results.push_back(casted);
            }
            for (const auto& child : node->getChildren())
            {
                toVisit.push(child);
            }
        }
        return results;
    }

    virtual std::span<const std::shared_ptr<LogicalFunction>> getChildren() const = 0;

protected:
    explicit LogicalFunction(std::shared_ptr<DataType> stamp);
    LogicalFunction(const LogicalFunction& other);

    std::shared_ptr<DataType> stamp;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalFunction& function)
{
    return os << function.toString();
}
}

FMT_OSTREAM(NES::LogicalFunction);
