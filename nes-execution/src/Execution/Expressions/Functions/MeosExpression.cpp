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

#include <Execution/Expressions/Functions/MeosExpression.hpp>
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace NES::Runtime::Execution::Expressions {


MeosExpression::MeosExpression(const ExpressionPtr& left, const ExpressionPtr& middle, const ExpressionPtr& right)
    : left(left), middle(middle), right(right) {}




std::string convertSecondsToTimestamp(long long seconds) {
    // Convert seconds to time_point
    std::chrono::seconds sec(seconds);
    std::chrono::time_point<std::chrono::system_clock> tp(sec);

    // Convert to time_t for formatting
    std::time_t time = std::chrono::system_clock::to_time_t(tp);

    // Convert to local time
    std::tm local_tm = *std::localtime(&time);

    // Format the time as a string
    std::ostringstream oss;
    oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}


/**
 * @brief This method calculates the log2 of x.
 * This function is basically a wrapper for std::log2 and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double meosT(float lon, float lat, int t) {
    // Implement your logic here
    NES_INFO("meosT called with lon: {}, lat: {}, t: {}", lon, lat, t);
    meos_initialize("UTC", NULL);
    NES_INFO("meos initialized");

    STBox *stbx =stbox_in("SRID=4326;STBOX X((3.3615, 53.964367),(16.505853, 59.24544))");
    NES_INFO("STBox created");
    std::string t_out = convertSecondsToTimestamp(t);
    std::string str_pointbuffer = std::format("SRID=4326;POINT({} {})@{}", lon, lat, t_out);
    NES_INFO("Point buffer created {}", str_pointbuffer);

    // if (eintersects_tpoint_geo((const Temporal *)planes[i].trip, continents[0].geom)){
    //     NES_INFO("Intersects");
    //     return true;
    // } else {
    //     NES_INFO("Does not intersect");
    //     return false;
    // }

    return true;
}

Value<> MeosExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = left->execute(record);
    Value middleValue = middle->execute(record);
    Value rightValue = right->execute(record);

    NES_INFO("Executing MeosExpression types: left: {}, middle: {}, right: {}", leftValue->getType()->toString(), middleValue->getType()->toString(), rightValue->getType()->toString());

     if (leftValue->isType<Double>()) {
        return Nautilus::FunctionCall<>("meosT", meosT, leftValue.as<Double>(), middleValue.as<Double>(), rightValue.as<Int64>());

    } else {
        // Throw an exception if no type is applicable
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<TernaryFunctionProvider<MeosExpression>> MeosExpression("meosT");

}// namespace NES::Runtime::Execution::Expressions
