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

#include <cstdint>
#include <expected>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Types/NESTypeCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

struct SampleTrait
{
    int rank = 0;
};

struct OtherTrait
{
    std::string label;
};

struct IntDescriptor
{
    int bits = 32;
    bool operator==(const IntDescriptor&) const = default;
};

struct ArrayDescriptor
{
    NESType elementType;
    int64_t length = 0;
    bool operator==(const ArrayDescriptor&) const = default;
};

class NESTypeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NESTypeTest.log", LogLevel::LOG_DEBUG);
    }
};

TEST_F(NESTypeTest, FlyweightTypeEqualityTraitsAndValueConstruction)
{
    const auto i32 = NESType::create<IntDescriptor, int64_t>(
                         IntDescriptor{.bits = 32},
                         [](const IntDescriptor&, const NESValueParameters& params) -> std::expected<int64_t, Exception>
                         {
                             if (params.size() != 1)
                             {
                                 return std::unexpected{InvalidQuerySyntax("Expected 1 arg")};
                             }
                             return params.at(0)->getAs<int64_t>();
                         })
                         .withTrait(SampleTrait{.rank = 4});

    const auto i64 = NESType::createUnconstructible<IntDescriptor, int64_t>(IntDescriptor{.bits = 64});

    EXPECT_EQ(i32, i32);
    EXPECT_NE(i32, i64);
    ASSERT_TRUE(i32.hasTrait<SampleTrait>());
    EXPECT_FALSE(i32.hasTrait<OtherTrait>());
    EXPECT_EQ(i32.tryGetTrait<SampleTrait>()->rank, 4);

    const auto rawArg = NESValue::create(i64, int64_t{42});
    const auto v1 = i32.construct(NESValueParameters::positional({rawArg}));
    const auto v2 = i32.construct(NESValueParameters::positional({rawArg}));
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(*v1, *v2);
    EXPECT_EQ(v1->getAs<int64_t>(), 42);

    const auto badConstruct = i32.construct();
    EXPECT_FALSE(badConstruct.has_value());
}

TEST_F(NESTypeTest, TypeClassCombinatorsAndParameterizedArrayFunctor)
{
    const auto scalarClass = NESTypeClass::forTrait<SampleTrait>();
    const auto otherClass = NESTypeClass::forTrait<OtherTrait>();
    const auto bothClass = scalarClass && otherClass;
    const auto eitherClass = scalarClass || otherClass;

    const auto elemType = NESType::createUnconstructible<IntDescriptor>(IntDescriptor{.bits = 32}).withTrait(SampleTrait{.rank = 1});
    const auto nonScalarType = NESType::createUnconstructible<IntDescriptor>(IntDescriptor{.bits = 16}).withTrait(OtherTrait{"x"});

    EXPECT_TRUE(scalarClass.validate(elemType));
    EXPECT_FALSE(bothClass.validate(elemType));
    EXPECT_TRUE(eitherClass.validate(elemType));
    EXPECT_TRUE(eitherClass.validate(nonScalarType));

    const auto intLiteralType = NESType::createUnconstructible<IntDescriptor, int64_t>(IntDescriptor{.bits = 64});
    const NESTypeClass positiveSizeClass{
        IntDescriptor{.bits = 64},
        [intLiteralType](const IntDescriptor&, const NESType& t) { return t == intLiteralType; }};

    const ExpectedNESTypeArguments expectedArgs{Schema<ExpectedNESTypeParameter, Ordered>{
        ExpectedNESTypeParameter{
            .name = QualifiedIdentifier::parse("ELEMENT_TYPE"),
            .constraint = scalarClass,
            .kind = ExpectedNESTypeParameter::Kind::TypeParameter},
        ExpectedNESTypeParameter{
            .name = QualifiedIdentifier::parse("SIZE"),
            .constraint = positiveSizeClass,
            .kind = ExpectedNESTypeParameter::Kind::ValueParameter}}};

    const NESFunctor fixedSizeArrayFunctor{
        std::string{"FixedSizeArray"},
        [](const std::string& a, const std::string& b) { return a == b; },
        [](const std::string&, const NESTypeParameters& bound) -> std::expected<NESType, Exception>
        {
            const auto element = bound.getTypeByName(QualifiedIdentifier::parse("ELEMENT_TYPE")).value();
            const auto lenVal = bound.getValueByName(QualifiedIdentifier::parse("SIZE")).value();
            const auto len = lenVal.getAs<int64_t>();
            if (len <= 0)
            {
                return std::unexpected{TypeInferenceException("FixedSizeArray length must be positive")};
            }
            return NESType::create<ArrayDescriptor, std::vector<NESValue>>(
                       ArrayDescriptor{.elementType = element, .length = len},
                       [](const ArrayDescriptor& desc, const NESValueParameters& vals)
                           -> std::expected<std::vector<NESValue>, Exception>
                       {
                           if (vals.size() != static_cast<size_t>(desc.length))
                           {
                               return std::unexpected{InvalidQuerySyntax("Array literal length mismatch")};
                           }
                           std::vector<NESValue> out;
                           for (size_t i = 0; i < vals.size(); ++i)
                           {
                               auto item = vals.at(i).value();
                               if (item.getType() != desc.elementType)
                               {
                                   return std::unexpected{TypeInferenceException("Array element type mismatch")};
                               }
                               out.push_back(std::move(item));
                           }
                           return out;
                       })
                .withTrait(SampleTrait{.rank = 2});
        },
        expectedArgs};

    const auto size5 = NESValue::create(intLiteralType, int64_t{5});
    const auto arr5Exp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{elemType, size5}});
    ASSERT_TRUE(arr5Exp.has_value());
    EXPECT_TRUE(scalarClass.validate(*arr5Exp));

    /// Nested array-of-array works because FixedSizeArray itself satisfies SampleTrait!
    const auto size2 = NESValue::create(intLiteralType, int64_t{2});
    const auto nestedArrExp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{*arr5Exp, size2}});
    ASSERT_TRUE(nestedArrExp.has_value());
    EXPECT_NE(*arr5Exp, *nestedArrExp);

    /// Rejects non-scalar element type via ExpectedNESTypeArguments constraint
    const auto rejectedExp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{nonScalarType, size5}});
    EXPECT_FALSE(rejectedExp.has_value());
}

}
}
