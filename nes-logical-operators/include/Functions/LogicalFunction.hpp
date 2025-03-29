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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <API/Schema.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Common.hpp>

namespace NES
{

/// @brief this indicates an function, which is a parameter for a FilterOperator or a MapOperator.
/// Each function declares a stamp, which expresses the data type of this function.
/// A stamp can be of a concrete type or invalid if the data type was not yet inferred.
class LogicalFunction : std::enable_shared_from_this<LogicalFunction>
{
public:
    explicit LogicalFunction(std::shared_ptr<DataType> stamp, std::string type);
    virtual ~LogicalFunction() = default;

    /// @brief Indicates if this function is a predicate -> if its result stamp is a boolean
    [[nodiscard]] bool isPredicate() const;

    /// Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(std::shared_ptr<Schema> schema);

    std::shared_ptr<DataType> getStamp() const;
    void setStamp(std::shared_ptr<DataType> stamp);

    virtual bool equal(std::shared_ptr<LogicalFunction> const& rhs) const = 0;

    virtual std::shared_ptr<LogicalFunction> clone() const = 0;

    virtual std::string toString() const  = 0;

    const std::string& getType() const
    {
        return type;
    }


    template <class FunctionType>
    void getFunctionByTypeHelper(std::vector<std::shared_ptr<FunctionType>>& foundNodes)
    {
        auto sharedThis = this->shared_from_this();
        if (NES::Util::instanceOf<FunctionType>(sharedThis))
        {
            foundNodes.push_back(NES::Util::as<FunctionType>(sharedThis));
        }
        for (auto& successor : this->children)
        {
            successor->getFunctionByTypeHelper(foundNodes);
        }
    };

    template <class FunctionType>
    std::vector<std::shared_ptr<FunctionType>> getFunctionByType()
    {
        std::vector<std::shared_ptr<FunctionType>> vector;
        getFunctionByTypeHelper<FunctionType>(vector);
        return vector;
    }

    std::vector<std::shared_ptr<LogicalFunction>> getChildren() const
    {
        return children;
    }

protected:
    explicit LogicalFunction(const LogicalFunction* other);

    std::vector<std::shared_ptr<LogicalFunction>> children;

    std::shared_ptr<DataType> stamp;
    std::string type;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalFunction& function)
{
    return os << function.toString();
}
}

template <typename T>
requires(std::is_base_of_v<NES::LogicalFunction, T>)
struct fmt::formatter<T> : fmt::ostream_formatter
{
};
