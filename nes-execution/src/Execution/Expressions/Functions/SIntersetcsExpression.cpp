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

#include <Execution/Expressions/Functions/SIntersetcsExpression.hpp>
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace NES::Runtime::Execution::Expressions {


SIntersetcsExpression::SIntersetcsExpression(const ExpressionPtr& one, const ExpressionPtr& two, const ExpressionPtr& three, const ExpressionPtr& four, const ExpressionPtr& five, const ExpressionPtr& six)
    : one(one),two(two),three(three),four(four),five(five),six(six) {}


std::string convertSecondsToTimestampMEOS(long long seconds) {
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
 * @brief This method if the given point is within the given STBox at the given time.
 * This function is basically a wrapper for MEOS ever intersects function.
 * @param lon Longitude of the point
* @param lat Latitude of the point
* @param t Time of the point
 * @return true if the point is within the STBox at the given time, false otherwise
 */
double seintersects(double lon, double lat, int t, int lon2, int lat2, int t2) {
    meos_initialize("UTC", NULL);
    std::string t_out = convertSecondsToTimestampMEOS(t);
    std::string str_pointbuffer = fmt::format("SRID=4326;POINT({} {})@{}", lon, lat, t_out);

    std::string t_out2 = convertSecondsToTimestampMEOS(t2);
    std::string str_pointbuffer2 = fmt::format("SRID=4326;POINT({} {})@{}", lon2, lat2, t_out);
    NES_INFO("Point buffer created {}", str_pointbuffer);
    TInstant *inst = (TInstant *)tgeompoint_in(str_pointbuffer.c_str());
    TInstant *inst2 = (TInstant *)tgeompoint_in(str_pointbuffer2.c_str());

    if (eintersects_tpoint_tpoint((const Temporal *)inst, (const Temporal *)inst2)){
        NES_INFO("Intersects");
        return 1;
    } else {
        NES_INFO("Does not intersect");
        return 0;
    }

    return 1;
}

Value<> SIntersetcsExpression::execute(NES::Nautilus::Record& record) const {
    Value oneValue = one->execute(record);
    Value twoValue = two->execute(record);
    Value threeValue = three->execute(record);
    Value fourValue = four->execute(record);
    Value fiveValue = five->execute(record);
    Value sixValue = six->execute(record);

    //NES_INFO("Executing SIntersetcsExpression types: {}, {}, {}, {}, {}, {}", oneValue->getType(), twoValue->getType(), threeValue->getType(), fourValue->getType(), fiveValue->getType(), sixValue->getType());

     if (oneValue->isType<Double>()) {
        return Nautilus::FunctionCall<>("seintersects", seintersects, oneValue.as<Double>(), twoValue.as<Double>(), threeValue.as<UInt64>(), fourValue.as<Double>(), fiveValue.as<Double>(), sixValue.as<UInt64>());

    } else {
        // Throw an exception if no type is applicable
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are Double, Double and Unsigned int.");
    }
}
static ExecutableFunctionRegistry::Add<SenaryFunctionProvider<SIntersetcsExpression>> SIntersetcsExpression("seintersects");

}// namespace NES::Runtime::Execution::Expressions
