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

#include <array>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
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

struct SampleTypeTrait
{
    int rank = 0;
};

struct OtherTypeTrait
{
    std::string label;
};

struct ConstructibleIntNESType
{
    int bits = 32;
    bool operator==(const ConstructibleIntNESType&) const = default;

    [[nodiscard]] static std::expected<int64_t, Exception> construct(const NESValueParameters& params)
    {
        if (params.size() != 1)
        {
            return std::unexpected{InvalidQuerySyntax("Expected 1 arg")};
        }
        return params.at(0)->getAs<int64_t>();
    }
};

struct UnconstructibleIntNESType
{
    using ValueType = int64_t;
    int bits = 64;
    bool operator==(const UnconstructibleIntNESType&) const = default;
};

struct FixedSizeArrayNESType
{
    NESType elementType;
    int64_t length = 0;
    bool operator==(const FixedSizeArrayNESType&) const = default;

    [[nodiscard]] std::expected<std::vector<NESValue>, Exception> construct(const NESValueParameters& vals) const
    {
        if (vals.size() != static_cast<size_t>(length))
        {
            return std::unexpected{InvalidQuerySyntax("Array literal length mismatch")};
        }
        std::vector<NESValue> out;
        for (size_t i = 0; i < vals.size(); ++i)
        {
            auto item = vals.at(i).value();
            if (item.getType() != elementType)
            {
                return std::unexpected{TypeInferenceException("Array element type mismatch")};
            }
            out.push_back(std::move(item));
        }
        return out;
    }
};

struct ExactNESTypeClass
{
    NESType expected;
    bool operator==(const ExactNESTypeClass&) const = default;
    [[nodiscard]] bool validate(const NESType& candidate) const { return candidate == expected; }
};

struct FixedSizeArrayFunctor
{
    const NESTypeParameter* elementTypeParam;
    const NESNonTypeParameter<int64_t>* sizeParam;

    bool operator==(const FixedSizeArrayFunctor& other) const = default;

    [[nodiscard]] ExpectedNESTypeArguments getExpectedArguments() const
    {
        return createExpectedNESTypeArguments(*elementTypeParam, *sizeParam);
    }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const InstantiatedNESTypeParameters& bound) const
    {
        const auto element = bound.get(*elementTypeParam);
        const auto len = bound.get(*sizeParam);
        if (len <= 0)
        {
            return std::unexpected{TypeInferenceException("FixedSizeArray length must be positive")};
        }
        return NESType{FixedSizeArrayNESType{.elementType = element, .length = len}}
            .withTypeTrait(SampleTypeTrait{.rank = 2});
    }
};

class NESTypeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NESTypeTest.log", LogLevel::LOG_DEBUG);
    }
};

TEST_F(NESTypeTest, FlyweightTypeEqualityTypeTraitsAndValueConstruction)
{
    const auto i32 = NESType{ConstructibleIntNESType{.bits = 32}}.withTypeTrait(SampleTypeTrait{.rank = 4});
    const auto i64 = NESType{UnconstructibleIntNESType{.bits = 64}};

    EXPECT_EQ(i32, i32);
    EXPECT_NE(i32, i64);
    ASSERT_NE(i32.tryGetAs<ConstructibleIntNESType>(), nullptr);
    EXPECT_EQ(i32.tryGetAs<ConstructibleIntNESType>()->bits, 32);
    ASSERT_TRUE(i32.hasTypeTrait<SampleTypeTrait>());
    EXPECT_FALSE(i32.hasTypeTrait<OtherTypeTrait>());
    EXPECT_EQ(i32.tryGetTypeTrait<SampleTypeTrait>()->rank, 4);

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
    const auto scalarClass = NESTypeClass::forTypeTrait<SampleTypeTrait>();
    const auto otherClass = NESTypeClass::forTypeTrait<OtherTypeTrait>();
    const auto bothClass = scalarClass && otherClass;
    const auto eitherClass = scalarClass || otherClass;

    const auto elemType = NESType{UnconstructibleIntNESType{.bits = 32}}.withTypeTrait(SampleTypeTrait{.rank = 1});
    const auto nonScalarType = NESType{UnconstructibleIntNESType{.bits = 16}}.withTypeTrait(OtherTypeTrait{"x"});

    EXPECT_TRUE(scalarClass.validate(elemType));
    EXPECT_FALSE(bothClass.validate(elemType));
    EXPECT_TRUE(eitherClass.validate(elemType));
    EXPECT_TRUE(eitherClass.validate(nonScalarType));

    const auto intLiteralType = NESType{UnconstructibleIntNESType{.bits = 64}};
    const NESTypeClass positiveSizeClass{ExactNESTypeClass{.expected = intLiteralType}};

    const NESTypeParameter ELEMENT_TYPE{Identifier::parse("ELEMENT_TYPE"), "Array element type", scalarClass};
    const NESNonTypeParameter<int64_t> SIZE{Identifier::parse("SIZE"), "Array element count", positiveSizeClass};

    const NESFunctor fixedSizeArrayFunctor{
        FixedSizeArrayFunctor{.elementTypeParam = &ELEMENT_TYPE, .sizeParam = &SIZE}};

    const auto size5 = NESValue::create(intLiteralType, int64_t{5});
    const auto arr5Exp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{elemType, size5}});
    ASSERT_TRUE(arr5Exp.has_value());
    EXPECT_TRUE(scalarClass.validate(*arr5Exp));

    /// Nested array-of-array works because FixedSizeArray itself carries SampleTypeTrait!
    const auto size2 = NESValue::create(intLiteralType, int64_t{2});
    const auto nestedArrExp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{*arr5Exp, size2}});
    ASSERT_TRUE(nestedArrExp.has_value());
    EXPECT_NE(*arr5Exp, *nestedArrExp);

    /// Rejects non-scalar element type via ExpectedNESTypeArguments constraint
    const auto rejectedExp = fixedSizeArrayFunctor.instantiate(NESTypeParameters{{nonScalarType, size5}});
    EXPECT_FALSE(rejectedExp.has_value());
}

TEST_F(NESTypeTest, FlyweightCopyEqualityAndDuplicateNamedParameterRejection)
{
    struct NonComparablePayloadType
    {
        using ValueType = std::function<int(int)>;
        std::shared_ptr<const int> token = std::make_shared<const int>(1);
        bool operator==(const NonComparablePayloadType&) const = default;
    };

    struct SimpleFunctor
    {
        int id = 7;
        bool operator==(const SimpleFunctor&) const = default;
        [[nodiscard]] static std::expected<NESType, Exception> instantiate(const NESTypeParameters&)
        {
            return NESType{UnconstructibleIntNESType{.bits = 32}};
        }
    };

    const NESFunctor functor{SimpleFunctor{.id = 7}};
    const auto functorCopy = functor;
    EXPECT_EQ(functor, functorCopy);
    ASSERT_NE(functor.tryGetAs<SimpleFunctor>(), nullptr);
    EXPECT_EQ(functor.tryGetAs<SimpleFunctor>()->id, 7);

    const auto tc = NESTypeClass::forTypeTrait<SampleTypeTrait>();
    const auto tcCopy = tc;
    EXPECT_EQ(tc, tcCopy);

    const auto ptc = NESParametrizedTypeClass::constant(tc);
    const auto ptcCopy = ptc;
    EXPECT_EQ(ptc, ptcCopy);

    const auto nonComparableType = NESType{NonComparablePayloadType{}};
    const auto val = NESValue::create(nonComparableType, std::function<int(int)>{[](const int x) { return x + 1; }});
    const auto valCopy = val;
    EXPECT_EQ(val, valCopy);

    /// Duplicate named arguments are rejected by ExpectedNESValueArguments::resolve and ExpectedNESTypeArguments::resolve
    const auto intType = NESType{UnconstructibleIntNESType{.bits = 64}};
    const auto v1 = NESValue::create(intType, int64_t{1});
    const auto v2 = NESValue::create(intType, int64_t{2});
    const Schema<NamedNESValue, Ordered> dupValueSchema{
        NamedNESValue{QualifiedIdentifier::parse("A"), v1},
        NamedNESValue{QualifiedIdentifier::parse("A"), v2}};
    const NESValueParameter<int64_t> VAL_A{Identifier::parse("A"), "First int"};
    const NESValueParameter<int64_t> VAL_B{
        Identifier::parse("B"),
        "Optional second int",
        NESTypeClass::any(),
        [](const NESValue& val) -> std::expected<int64_t, Exception> { return val.getAs<int64_t>(); },
        int64_t{99}};
    const auto expectedVals = createExpectedNESValueArguments(VAL_A, VAL_B);
    EXPECT_FALSE(expectedVals.resolve(NESValueParameters::named(dupValueSchema)).has_value());

    const auto resolvedWithDefault = expectedVals.resolve(NESValueParameters::positional({v1}));
    ASSERT_TRUE(resolvedWithDefault.has_value());
    EXPECT_EQ(resolvedWithDefault->get(VAL_A), 1);
    EXPECT_EQ(resolvedWithDefault->get(VAL_B), 99);

    const Schema<NamedNESTypeParameter, Ordered> dupTypeParamSchema{
        NamedNESTypeParameter{QualifiedIdentifier::parse("A"), intType},
        NamedNESTypeParameter{QualifiedIdentifier::parse("A"), intType}};
    const NESTypeParameter PARAM_A{Identifier::parse("A"), "Required type parameter"};
    const NESOptionalTypeParameter PARAM_B{Identifier::parse("B"), "Optional type parameter"};
    const auto expected = createExpectedNESTypeArguments(PARAM_A, PARAM_B);
    EXPECT_FALSE(expected.resolve(NESTypeParameters::named(dupTypeParamSchema)).has_value());

    const auto resolvedTypeOnlyA = expected.resolve(NESTypeParameters::fromTypes({intType}));
    ASSERT_TRUE(resolvedTypeOnlyA.has_value());
    EXPECT_EQ(resolvedTypeOnlyA->get(PARAM_A), intType);
    EXPECT_FALSE(resolvedTypeOnlyA->get(PARAM_B).has_value());

    /// Higher-kinded NESFunctorParameter and NESOptionalFunctorParameter resolution
    const NESFunctor altFunctor{SimpleFunctor{.id = 42}};
    const NESFunctorParameter FUNCTOR_REQ{Identifier::parse("F"), "Required functor"};
    const NESFunctorParameter FUNCTOR_DEF{Identifier::parse("G"), "Defaulted functor", altFunctor};
    const NESOptionalFunctorParameter FUNCTOR_OPT{Identifier::parse("H"), "Optional functor"};
    const auto expectedFunctors = createExpectedNESTypeArguments(FUNCTOR_REQ, FUNCTOR_DEF, FUNCTOR_OPT);

    EXPECT_FALSE(expectedFunctors.resolve(NESTypeParameters::fromTypes({intType})).has_value());
    EXPECT_FALSE(expectedFunctors.resolve(NESTypeParameters::fromValues({v1})).has_value());

    const auto resolvedFunctorDefaults = expectedFunctors.resolve(NESTypeParameters::fromFunctors({functor}));
    ASSERT_TRUE(resolvedFunctorDefaults.has_value());
    EXPECT_EQ(resolvedFunctorDefaults->get(FUNCTOR_REQ), functor);
    EXPECT_EQ(resolvedFunctorDefaults->get(FUNCTOR_DEF), altFunctor);
    EXPECT_FALSE(resolvedFunctorDefaults->get(FUNCTOR_OPT).has_value());

    const auto resolvedNamedFunctors = expectedFunctors.resolve(NESTypeParameters::named(Schema<NamedNESTypeParameter, Ordered>{
        NamedNESTypeParameter{QualifiedIdentifier::parse("F"), functor},
        NamedNESTypeParameter{QualifiedIdentifier::parse("H"), altFunctor}}));
    ASSERT_TRUE(resolvedNamedFunctors.has_value());
    EXPECT_EQ(resolvedNamedFunctors->get(FUNCTOR_REQ), functor);
    EXPECT_EQ(resolvedNamedFunctors->get(FUNCTOR_DEF), altFunctor);
    ASSERT_TRUE(resolvedNamedFunctors->get(FUNCTOR_OPT).has_value());
    EXPECT_EQ(*resolvedNamedFunctors->get(FUNCTOR_OPT), altFunctor);
}

}
}
