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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_

#include <Nautilus/IR/Types/Stamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <absl/base/config.h>
#include <cstdint>
#include <fmt/core.h>
#include <functional>
#include <gsl/pointers.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::Nautilus::IR::Types {
class Stamp;
using StampPtr = std::shared_ptr<Stamp>;
}// namespace NES::Nautilus::IR::Types

namespace NES::Nautilus::IR::Operations {
using OperationIdentifier = std::string;
class Operation {
  public:
    enum class ProxyCallType : uint8_t { GetNumTuples = 0, SetNumTuples = 1, GetDataBuffer = 2, Other = 50 };
    enum class OperationType : uint8_t {
        AddOp,
        AddressOp,
        AndOp,
        BitWiseAnd,
        BitWiseOr,
        BitWiseXor,
        BitWiseLeftShift,
        BitWiseRightShift,
        BasicBlockArgument,
        BlockInvocation,
        BranchOp,
        ConstIntOp,
        ConstBooleanOp,
        ConstFloatOp,
        CastOp,
        CompareOp,
        DivOp,
        ModOp,
        FunctionOp,
        IfOp,
        LoopOp,
        LoadOp,
        MulOp,
        MLIR_YIELD,
        NegateOp,
        OrOp,
        ProxyCallOp,
        ReturnOp,
        StoreOp,
        SubOp,
    };

    explicit Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp);
    explicit Operation(OperationType opType, Types::StampPtr stamp);

    Operation(const Operation& other) = delete;
    Operation(Operation&& other) noexcept = delete;
    Operation& operator=(const Operation& other) = delete;
    Operation& operator=(Operation&& other) noexcept = delete;

    virtual ~Operation() = default;
    [[nodiscard]] OperationIdentifier getIdentifier() const;
    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] OperationType getOperationType() const;
    [[nodiscard]] const Types::StampPtr& getStamp() const;
    void addUsage(const Operation&);
    [[nodiscard]] const std::vector<gsl::not_null<const Operation*>>& getUsages() const;

  protected:
    OperationType opType;
    OperationIdentifier identifier;
    const Types::StampPtr stamp;
    std::vector<gsl::not_null<const Operation*>> usages;
};
using OperationPtr = std::unique_ptr<Operation>;
using OperationRef = gsl::not_null<Operation*>;

}// namespace NES::Nautilus::IR::Operations

namespace fmt {
template<>
struct formatter<NES::Nautilus::IR::Operations::Operation> : formatter<std::string> {
    auto format(const NES::Nautilus::IR::Operations::Operation& operation, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{}", operation.toString());
    }
};
}// namespace fmt
#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_
