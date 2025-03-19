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
#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES {
/// The `Operator` struct holds all the state for an operator in the query plan.
struct Operator {
    explicit Operator();
    /// Virtual destructor to make `Operator` polymorphic
    virtual ~Operator() = default;
    [[nodiscard]] virtual std::string toString() const = 0;
    friend std::ostream& operator<<(std::ostream& os, const Operator& op);
    /// Compares operator content ignoring 'id'
    [[nodiscard]] virtual bool operator==(const Operator& rhs) const;
    /// Expose children as a non-owning view.
    [[nodiscard]] std::span<const std::unique_ptr<Operator>> getChildren() const;
    /// Transfers ownership of children
    [[nodiscard]] std::vector<std::unique_ptr<Operator>> releaseChildren() const;
    /// Creates a new owing instance. Clone has a new operator id.
    [[nodiscard]] std::unique_ptr<Operator> clone() const;
    /// Unique identifier for the operator during runtime
    const OperatorId id;
    /// A operator owns it children and has non-owning pointers to its parents
    std::vector<Operator*> parents;
    std::vector<std::unique_ptr<Operator>> children;

    static std::string getName()
    {
        return "Operator";
    }

protected:
    /// Each derived operator implements this to clone its own state
    virtual std::unique_ptr<Operator> cloneImpl() const = 0;
};
}
FMT_OSTREAM(NES::Operator);
