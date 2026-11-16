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

#include <LogicalOperatorRegistry.hpp>

#include <expected>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct LogicalOperatorValueTag
{
    bool operator==(const LogicalOperatorValueTag&) const = default;
};

struct LogicalOperatorParametrizedTag
{
    bool operator==(const LogicalOperatorParametrizedTag&) const = default;
};
}

NESValue makeLogicalOperatorValue(LogicalOperator op)
{
    static const NESType opType
        = NESType::createUnconstructible<LogicalOperatorValueTag, LogicalOperator>(LogicalOperatorValueTag{})
              .withTrait(LogicalOperatorTrait{});
    return NESValue::create(opType, std::move(op));
}

LogicalOperatorRegistry& LogicalOperatorRegistry::instance()
{
    static LogicalOperatorRegistry inst;
    return inst;
}

NESTypeClass LogicalOperatorRegistry::logicalOperatorTypeClass()
{
    return NESTypeClass::forTrait<LogicalOperatorTrait>();
}

NamedNESParametrizedTypeClass LogicalOperatorRegistry::getParametrizedTypeClass()
{
    return NamedNESParametrizedTypeClass{
        QualifiedIdentifier::parse("LogicalOperator"),
        NESParametrizedTypeClass{
            LogicalOperatorParametrizedTag{},
            [](const LogicalOperatorParametrizedTag&, const NESTypeClassParameters& params) -> std::expected<NESTypeClass, Exception>
            {
                if (params.size() > 1)
                {
                    return std::unexpected{
                        TypeInferenceException("LogicalOperator type class accepts at most 1 optional schema parameter")};
                }
                if (params.size() == 1)
                {
                    const auto& p = params.getParameters();
                    const auto valOpt = p.isNamed() ? p.getValueByName(QualifiedIdentifier::parse("SCHEMA")) : p.getValueAt(0);
                    if (!valOpt.has_value() || valOpt->tryGetAs<Schema<UnqualifiedUnboundField, Ordered>>() == nullptr)
                    {
                        return std::unexpected{
                            TypeInferenceException("LogicalOperator type class parameter must be a Schema")};
                    }
                }
                return logicalOperatorTypeClass();
            }}};
}

std::vector<NamedNESParametrizedTypeClass> LogicalOperatorRegistry::getParametrizedTypeClasses()
{
    return {getParametrizedTypeClass()};
}

}
