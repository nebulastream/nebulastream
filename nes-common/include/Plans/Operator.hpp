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
    /// Needed for concepts requiring getChildren func.
    [[nodiscard]] std::span<const std::shared_ptr<Operator>> getChildren() const;
    /// Unique identifier for the operator during runtime
    const OperatorId id;
    std::vector<std::shared_ptr<Operator>> children;
    std::vector<std::shared_ptr<Operator>> parents;
};
}
FMT_OSTREAM(NES::Operator);
