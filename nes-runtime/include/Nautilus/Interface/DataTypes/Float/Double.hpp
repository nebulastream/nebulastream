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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_DOUBLE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_DOUBLE_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
namespace NES::Nautilus {

/**
 * @brief Double data type.
 */
class Double : public TraceableType {
  public:
    using RawType = double;
    static const inline auto type = TypeIdentifier::create<Double>();
    Double(double value);
    ~Double() {}
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    std::shared_ptr<Double> add(const Double& otherValue) const;
    std::shared_ptr<Double> sub(const Double& otherValue) const;
    std::shared_ptr<Double> mul(const Double& otherValue) const;
    std::shared_ptr<Double> div(const Double& otherValue) const;
    std::shared_ptr<Boolean> equals(const Double& otherValue) const;
    std::shared_ptr<Boolean> lessThan(const Double& otherValue) const;
    std::shared_ptr<Boolean> greaterThan(const Double& otherValue) const;
    double getValue() const;
    std::string toString() override;

  private:
    const double value;
};
}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_DOUBLE_HPP_
