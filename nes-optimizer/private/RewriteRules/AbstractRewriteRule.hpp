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

#include <PhysicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>

namespace NES::Optimizer
{

struct AbstractRewriteRule
{
    virtual VirtualTraitSet* apply(VirtualTraitSet*) = 0;
    virtual ~AbstractRewriteRule() = default;
};

template <Trait... T>
struct TypedAbstractRewriteRule : AbstractRewriteRule
{
    VirtualTraitSet* apply(VirtualTraitSet* inputTS) override
    {
        return applyTyped(new DynamicTraitSet<T...>(inputTS));
    };

    virtual DynamicTraitSet<T...>* applyTyped(DynamicTraitSet<T...>*) = 0;
};

template <Trait... T>
struct AbstractLowerToPhysicalRewriteRule : public AbstractRewriteRule {
    AbstractTraitSet* apply(AbstractTraitSet* inputTS) override {
        auto typedTS = dynamic_cast<DynamicTraitSet<T...>*>(inputTS);
        if (!typedTS) {
            throw std::bad_cast();  // TODO
        }
        std::vector<std::shared_ptr<PhysicalOperator>> ops = applyToPhysical(typedTS);
        return new PhysicalOperatorTraitSet(std::move(ops));
    }

    // Derived classes will implement this to produce the physical operators.
    virtual std::vector<std::shared_ptr<PhysicalOperator>> applyToPhysical(DynamicTraitSet<T...>* typedTS) = 0;
    virtual ~AbstractLowerToPhysicalRewriteRule() = default;
};

}
