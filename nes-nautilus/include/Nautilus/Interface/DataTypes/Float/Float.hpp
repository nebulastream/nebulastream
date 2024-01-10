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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_FLOAT_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_FLOAT_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
namespace NES::Nautilus {

/**
 * @brief Float data type.
 */
class Float : public TraceableType {
  public:
    using RawType = float;
    static const inline auto type = TypeIdentifier::create<Float>();
    Float(float value);
    Nautilus::IR::Types::StampPtr getType() const override;
    std::shared_ptr<Any> copy() override;
    std::shared_ptr<Float> add(const Float& otherValue) const;
    std::shared_ptr<Float> sub(const Float& otherValue) const;
    std::shared_ptr<Float> mul(const Float& otherValue) const;
    std::shared_ptr<Float> div(const Float& otherValue) const;
    std::shared_ptr<Boolean> equals(const Float& otherValue) const;
    std::shared_ptr<Boolean> lessThan(const Float& otherValue) const;
    std::shared_ptr<Boolean> greaterThan(const Float& otherValue) const;
    float getValue() const;
    std::string toString() override;

  private:
    const float value;
};

}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_FLOAT_FLOAT_HPP_
