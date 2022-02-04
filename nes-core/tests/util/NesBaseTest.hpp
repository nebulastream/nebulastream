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
#ifndef NES_TESTS_UTIL_NESBASETEST_HPP_
#define NES_TESTS_UTIL_NESBASETEST_HPP_

#include <gtest/gtest.h>
#include <filesystem>

namespace NES {
    namespace Testing {
    class NesPortDispatcher;

    class BorrowedPort {
      private:
        uint16_t port;
        uint32_t portIndex;
        NesPortDispatcher* parent;

      public:
        explicit BorrowedPort(uint16_t port, uint32_t portIndex, NesPortDispatcher* parent) : port(port), portIndex(portIndex), parent(parent) {}

        ~BorrowedPort() noexcept;

        [[nodiscard]] inline operator uint16_t() const { return port; }
    };
    using BorrowedPortPtr = std::shared_ptr<BorrowedPort>;

    class NESBaseTest : public testing::Test {
        friend class BorrowedPort;

      protected:
        BorrowedPortPtr rpcCoordinatorPort{nullptr};
        BorrowedPortPtr restPort{nullptr};

      public:
        explicit NESBaseTest();

        void SetUp() override;

        void TearDown() override;

      protected:
        BorrowedPortPtr getAvailablePort();

        std::filesystem::path getTestResourceFolder() const;

      private:
        std::filesystem::path testResourcePath;

    };
    }// namespace Testing

}// namespace NES

#endif//NES_TESTS_UTIL_NESBASETEST_HPP_