
#include <Util/TimeMeasurement.hpp>

#include <chrono>
namespace iotdb {

using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

Timestamp getTimestamp()
{
    return static_cast<Timestamp>(std::chrono::duration_cast<NanoSeconds>(Clock::now().time_since_epoch()).count());
}

} // namespace iotdb
