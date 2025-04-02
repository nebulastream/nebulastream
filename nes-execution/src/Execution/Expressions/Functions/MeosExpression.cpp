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
 * @brief This method if the given point is within the given STBox at the given time.
 * This function is basically a wrapper for MEOS ever intersects function.
 * @param lon Longitude of the point
* @param lat Latitude of the point
* @param t Time of the point
 * @return true if the point is within the STBox at the given time, false otherwise
 */
double teintersects(double lon, double lat, int t) {
    meos_initialize("UTC", NULL);
    STBox* stbx = stbox_in("SRID=4326;STBOX X((3.5, 50.5),(4.5, 51.5))");
    GSERIALIZED *geom = stbox_to_geo(stbx);
    std::string t_out = convertSecondsToTimestamp(t);
    std::string str_pointbuffer = std::format("SRID=4326;POINT({} {})@{}", lon, lat, t_out);
    TInstant *inst = (TInstant *)tgeompoint_in(str_pointbuffer.c_str());

    if (eintersects_tpoint_geo((const Temporal *)inst, geom))
        return 1;

    return 0;
}

Value<> MeosExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = left->execute(record);
    Value middleValue = middle->execute(record);
    Value rightValue = right->execute(record);

    //NES_INFO("Executing MeosExpression types: left: {}, middle: {}, right: {}", leftValue->getType()->toString(), middleValue->getType()->toString(), rightValue->getType()->toString());

     if (leftValue->isType<Double>()) {
        return Nautilus::FunctionCall<>("teintersects", teintersects, leftValue.as<Double>(), middleValue.as<Double>(), rightValue.as<Int64>());

    } else {
        // Throw an exception if no type is applicable
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are Double, Double and Unsigned int.");
    }
}
static ExecutableFunctionRegistry::Add<TernaryFunctionProvider<MeosExpression>> MeosExpression("teintersects");

}// namespace NES::Runtime::Execution::Expressions
