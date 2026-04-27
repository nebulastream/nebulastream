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

#include <Thread.hpp>
#include <WorkerLocalSingleton.hpp>

#include <gtest/gtest.h>

namespace NES
{

namespace
{
struct DummyService : WorkerLocalSingleton<DummyService>
{
    int value;
    explicit DummyService(int v) : value(v) { }
};

struct OtherService : WorkerLocalSingleton<OtherService>
{
    const char* tag;
    explicit OtherService(const char* t) : tag(t) { }
};
}

TEST(WorkerLocalSingletonTest, NotSetByDefault)
{
    EXPECT_EQ(DummyService::tryInstance(), nullptr);
}

TEST(WorkerLocalSingletonTest, ConstructionPublishesInstance)
{
    {
        DummyService svc(42);
        ASSERT_NE(DummyService::tryInstance(), nullptr);
        EXPECT_EQ(DummyService::instance().value, 42);
    }
    EXPECT_EQ(DummyService::tryInstance(), nullptr);
}

TEST(WorkerLocalSingletonTest, NestedScopesRestorePrevious)
{
    DummyService outer(1);
    EXPECT_EQ(DummyService::instance().value, 1);
    {
        DummyService inner(2);
        EXPECT_EQ(DummyService::instance().value, 2);
    }
    EXPECT_EQ(DummyService::instance().value, 1);
}

TEST(WorkerLocalSingletonTest, MultipleTypesIndependent)
{
    DummyService dummy(7);
    OtherService other("hello");
    EXPECT_EQ(DummyService::instance().value, 7);
    EXPECT_STREQ(OtherService::instance().tag, "hello");
}

TEST(WorkerLocalSingletonTest, PropagatesToChildThread)
{
    DummyService svc(123);
    Thread t(
        "child",
        Host("w1"),
        []()
        {
            ASSERT_NE(DummyService::tryInstance(), nullptr);
            EXPECT_EQ(DummyService::instance().value, 123);
        });
}

TEST(WorkerLocalSingletonTest, PropagatesAcrossNestedThreads)
{
    DummyService svc(9);
    Thread outer(
        "outer",
        Host("w1"),
        []()
        {
            EXPECT_EQ(DummyService::instance().value, 9);
            Thread inner(
                "inner",
                []()
                {
                    EXPECT_EQ(DummyService::instance().value, 9);
                });
        });
}

TEST(WorkerLocalSingletonTest, ChildThreadSeesShadowedInstance)
{
    DummyService outer(1);
    {
        DummyService inner(2);
        Thread t(
            "child",
            Host("w1"),
            []()
            {
                EXPECT_EQ(DummyService::instance().value, 2);
            });
    }
    Thread t(
        "child2",
        Host("w1"),
        []()
        {
            EXPECT_EQ(DummyService::instance().value, 1);
        });
}

}
