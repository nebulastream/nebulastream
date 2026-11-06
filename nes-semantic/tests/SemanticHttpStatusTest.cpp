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

#include <chrono>
#include <optional>
#include <gtest/gtest.h>
#include <ResponseParsing.hpp>

namespace NES
{
namespace
{

TEST(SemanticHttpStatusTest, TooManyRequestsAndServerTroubleAreTransient)
{
    for (const unsigned status : {429U, 500U, 502U, 503U, 504U, 529U})
    {
        EXPECT_TRUE(isTransientStatus(status)) << status;
    }
    for (const unsigned status : {200U, 400U, 401U, 403U, 404U, 413U, 501U, 505U})
    {
        EXPECT_FALSE(isTransientStatus(status)) << status;
    }
}

TEST(SemanticHttpStatusTest, RetryAfterInSeconds)
{
    EXPECT_EQ(parseRetryAfter("3"), std::chrono::milliseconds{3000});
    EXPECT_EQ(parseRetryAfter(" 0 "), std::chrono::milliseconds{0});
    EXPECT_EQ(parseRetryAfter("1.5"), std::nullopt);
    EXPECT_EQ(parseRetryAfter("-1"), std::nullopt);
    EXPECT_EQ(parseRetryAfter(""), std::nullopt);
    EXPECT_EQ(parseRetryAfter("soon"), std::nullopt);
    EXPECT_EQ(parseRetryAfter("99999999999999999999"), std::nullopt);
}

TEST(SemanticHttpStatusTest, RetryAfterAsHttpDate)
{
    /// 21 Oct 2015 07:28:00 UTC.
    const auto now = std::chrono::system_clock::from_time_t(1445412480);
    EXPECT_EQ(parseRetryAfter("Wed, 21 Oct 2015 07:28:30 GMT", now), std::chrono::milliseconds{30000});
    EXPECT_EQ(parseRetryAfter("Wed, 21 Oct 2015 07:27:00 GMT", now), std::chrono::milliseconds{0}) << "a date in the past";
    EXPECT_EQ(parseRetryAfter("Wed, 21 Oct 2015 07:28:30 CET", now), std::nullopt);
}

}
}
