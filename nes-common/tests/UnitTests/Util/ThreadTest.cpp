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

#include <atomic>
#include <string>
#include <tuple>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

TEST(ThreadTest, ThreadNameIsSetOnChildThread)
{
    Thread t(
        "TestThread",
        []()
        {
            EXPECT_EQ(Thread::getThisThreadName(), "TestThread");
            Thread t1("InnerThread", []() { EXPECT_EQ(Thread::getThisThreadName(), "InnerThread"); });
        });
}

/// Test service that uses WorkerLocalSingleton.
struct TestService : WorkerLocalSingleton<TestService>
{
    int value;

    explicit TestService(int v) : value(v) { }
};

TEST(WorkerLocalSingletonTest, InstancePropagesToChildThread)
{
    Thread t(
        "Parent",
        []()
        {
            TestService svc(42);

            EXPECT_EQ(TestService::tryInstance(), &svc);
            EXPECT_EQ(TestService::instance().value, 42);

            {
                Thread child(
                    "Child",
                    []()
                    {
                        EXPECT_NE(TestService::tryInstance(), nullptr);
                        EXPECT_EQ(TestService::instance().value, 42);
                    });
            }
        });
}

TEST(WorkerLocalSingletonTest, PropagatesAcrossGenerations)
{
    Thread t(
        "GrandParent",
        []()
        {
            TestService svc(99);

            {
                Thread child(
                    "Parent",
                    []()
                    {
                        EXPECT_EQ(TestService::instance().value, 99);

                        Thread grandchild("GrandChild", []() { EXPECT_EQ(TestService::instance().value, 99); });
                    });
            }
        });
}

TEST(WorkerLocalSingletonTest, DestructionRestoresPrevious)
{
    Thread t(
        "Parent",
        []()
        {
            EXPECT_EQ(TestService::tryInstance(), nullptr);

            {
                TestService outer(1);
                EXPECT_EQ(TestService::instance().value, 1);
                {
                    TestService inner(2);
                    EXPECT_EQ(TestService::instance().value, 2);
                }
                /// inner destroyed — outer is restored
                EXPECT_EQ(TestService::instance().value, 1);
            }
            /// outer destroyed — nullptr restored
            EXPECT_EQ(TestService::tryInstance(), nullptr);
        });
}

TEST(WorkerLocalSingletonTest, TryInstanceReturnsNullWhenNotSet)
{
    Thread t("NoService", []() { EXPECT_EQ(TestService::tryInstance(), nullptr); });
}

TEST(WorkerLocalSingletonTest, ChildDoesNotAffectParent)
{
    Thread t(
        "Parent",
        []()
        {
            TestService parentSvc(10);

            {
                Thread child(
                    "Child",
                    []()
                    {
                        /// Child inherits parent's service
                        EXPECT_EQ(TestService::instance().value, 10);
                        /// Child creates its own
                        TestService childSvc(20);
                        EXPECT_EQ(TestService::instance().value, 20);
                    });
            }

            /// Parent still has its own service
            EXPECT_EQ(TestService::instance().value, 10);
        });
}

TEST(ThreadTest, IsNESThreadFalseByDefault)
{
    /// The main test thread is not created via NES::Thread.
    EXPECT_FALSE(detail::isNESThread);
}

TEST(ThreadTest, IsNESThreadTrueOnChildThread)
{
    Thread t(
        "NESChild",
        []()
        {
            EXPECT_TRUE(detail::isNESThread);

            Thread grandchild("NESGrandChild", []() { EXPECT_TRUE(detail::isNESThread); });
        });
}

TEST(WorkerLocalSingletonTest, InstanceFailsOnNonNESThread)
{
    SKIP_IF_TSAN();
    /// Main thread is not a NES thread and has no TestService — instance() should abort.
    EXPECT_DEATH(std::ignore = TestService::instance(), ".*");
}

TEST(WorkerLocalSingletonTest, InstanceFailsOnNESThreadWithoutSingleton)
{
    SKIP_IF_TSAN();
    /// NES thread without a TestService — instance() should abort.
    Thread t("NoSingleton", []() { EXPECT_DEATH(std::ignore = TestService::instance(), ".*"); });
}

TEST(WorkerLocalSingletonTest, WorkerNodeIdViaHookMap)
{
    Thread t(
        "WorkerRoot",
        []()
        {
            /// Simulate what SingleNodeWorker does.
            Thread::WorkerNodeId = Host("worker-42");
            detail::threadInitHooks[std::type_index(typeid(Host))] = []() { Thread::WorkerNodeId = Host("worker-42"); };

            Thread child(
                "ChildThread",
                []()
                {
                    EXPECT_EQ(Host("worker-42"), Thread::getThisWorkerNodeId());

                    Thread grandchild("GrandChildThread", []() { EXPECT_EQ(Host("worker-42"), Thread::getThisWorkerNodeId()); });
                });
        });
}
}
