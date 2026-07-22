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

// Include standard library headers first, before MEOS
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>

// Include MEOS wrapper after standard headers
#include <ErrorHandling.hpp>
#include <MEOSWrapper.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace MEOS
{

// Global MEOS initialization
static bool meos_initialized = false;
static std::mutex meos_init_mutex;
static std::mutex meos_parse_mutex;
static std::mutex meos_exec_mutex;

static void cleanupMeos()
{
    if (meos_initialized)
    {
        meos_finalize();
        meos_initialized = false;
    }
}

static void ensureMeosInitialized()
{
    std::lock_guard<std::mutex> lk(meos_init_mutex);
    if (!meos_initialized)
    {
        // Ensure a sane timezone environment before initializing MEOS (uses PostgreSQL tzdb)
        const char* tzEnv = std::getenv("TZ");
        if (!tzEnv || *tzEnv == '\0')
        {
            setenv("TZ", "UTC", 1);
        }
        // PGTZ is used by the underlying PG timezone code; prefer same value as TZ
        const char* pgtzEnv = std::getenv("PGTZ");
        if (!pgtzEnv || *pgtzEnv == '\0')
        {
            const char* tzNow = std::getenv("TZ");
            setenv("PGTZ", tzNow ? tzNow : "UTC", 1);
        }
        // Provide a tz database directory if none is set and a common system path exists
        const char* tzdirEnv = std::getenv("TZDIR");
        if (!tzdirEnv || *tzdirEnv == '\0')
        {
            namespace fs = std::filesystem;
            const char* candidates[] = {"/usr/share/zoneinfo", "/usr/lib/zoneinfo", "/usr/share/lib/zoneinfo"};
            for (const auto* cand : candidates)
            {
                std::error_code ec;
                if (fs::exists(cand, ec) && !ec)
                {
                    setenv("TZDIR", cand, 1);
                    break;
                }
            }
        }
        tzset();

        meos_initialize();
        meos_initialized = true;
        // Register cleanup function to be called at program exit
        std::atexit(cleanupMeos);
    }
}

Meos::Meos()
{
    ensureMeosInitialized();
}

Meos::~Meos()
{
    // Do not finalize MEOS here - it should remain initialized for the lifetime of the program
    //we finalize at cleanupMeos
}

std::string Meos::convertSecondsToTimestamp(long long seconds)
{
    // Use UTC to avoid timezone ambiguities and Docker tz issues
    std::chrono::seconds sec(seconds);
    std::chrono::time_point<std::chrono::system_clock> tp(sec);

    std::time_t time = std::chrono::system_clock::to_time_t(tp);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &time);
#else
    utc_tm = *std::gmtime(&time);
#endif

    std::ostringstream oss;
    // Append explicit UTC offset so MEOS parser reads a zoned timestamp
    oss << std::put_time(&utc_tm, "%Y-%m-%d %H:%M:%S") << "+00";
    return oss.str();
}

std::string Meos::convertEpochToTimestamp(unsigned long long epochLike)
{
    // Normalize to seconds using magnitude heuristics
    // >=1e19 -> ns, >=1e16 -> us, >=1e13 -> ms, else seconds
    unsigned long long secondsUll = epochLike;
    if (epochLike >= 1000000000000000000ULL)
    { // nanoseconds
        secondsUll = epochLike / 1000000000ULL;
    }
    else if (epochLike >= 1000000000000000ULL)
    { // microseconds
        secondsUll = epochLike / 1000000ULL;
    }
    else if (epochLike >= 1000000000000ULL)
    { // milliseconds
        secondsUll = epochLike / 1000ULL;
    }
    else
    {
        // seconds already
        secondsUll = epochLike;
    }

    // Clamp to a reasonable range to avoid tz library failures
    // Here we clamp to [0, 4102444800] ~ 2100-01-01 for safety
    const unsigned long long kMaxReasonable = 4102444800ULL; // 2100-01-01T00:00:00Z
    if (secondsUll > kMaxReasonable)
    {
        secondsUll = kMaxReasonable;
    }

    return convertSecondsToTimestamp(static_cast<long long>(secondsUll));
}

TimestampTz Meos::convertEpochToTimestampTz(unsigned long long epochLike)
{
    /// 1) Normalize to microseconds
    unsigned long long unixMicros;
    if (epochLike >= 1000000000000000000ULL)
    { // nanoseconds  (~1.7e18 today)
        unixMicros = epochLike / 1000ULL;
    }
    else if (epochLike >= 1000000000000000ULL)
    { /// microseconds (~1.7e15)
        unixMicros = epochLike;
    }
    else if (epochLike >= 1000000000000ULL)
    { /// milliseconds (~1.7e12)
        unixMicros = epochLike * 1000ULL;
    }
    else
    { /// seconds      (~1.7e9)
        unixMicros = epochLike * 1000000ULL;
    }

    /// Clamp in the Unix domain
    const unsigned long long kMaxUnixMicros = 4102444800000000ULL; /// 2100-01-01Z
    if (unixMicros > kMaxUnixMicros)
        unixMicros = kMaxUnixMicros;

    /// Re-base from Unix epoch (1970) to PostgreSQL/MEOS epoch (2000).
    const long long kPgEpochOffsetMicros = 946684800LL * 1000000LL; /// 946684800000000
    return static_cast<TimestampTz>(static_cast<long long>(unixMicros) - kPgEpochOffsetMicros);
}

// TemporalInstant constructor
Meos::TemporalInstant::TemporalInstant(double lon, double lat, long long ts, int srid)
{
    // Ensure MEOS is initialized
    ensureMeosInitialized();

    TimestampTz meosTimestamp = convertEpochToTimestampTz(ts);


    /// Creating the moving point directly out of the binary values without casting to a string and using the entry points for human readable
    /// saves value parse time
    Temporal* temp = nullptr;
    {
        std::lock_guard<std::mutex> lk(meos_parse_mutex);
        /// Create a geometric 2-dimensional point
        const GSERIALIZED* point = geompoint_make2d(srid, lon, lat);
        /// Apply timestamp to create a temporal instant
        temp = reinterpret_cast<Temporal*>(tgeoinst_make(point, meosTimestamp));
    }

    if (temp == nullptr)
    {
        // Try alternative format or set to null
        instant = nullptr;
    }
    else
    {
        instant = temp;
    }
}

Meos::TemporalInstant::~TemporalInstant()
{
    // Do not free here: lifetime managed by MEOS/PG memory context.
    // Avoid double-free or mismatched allocator issues across builds.
    instant = nullptr;
}

Temporal* Meos::TemporalInstant::getGeometry()
{
    return instant;
}

bool Meos::TemporalInstant::intersects(const TemporalInstant& point) const
{
    // Use MEOS eintersects function for temporal points  - this will change
    return eintersects_tgeo_tgeo((const Temporal*)this->instant, (const Temporal*)point.instant);
}

Meos::TemporalGeometry::TemporalGeometry(const std::string& wkt_string)
{
    ensureMeosInitialized();

    // Try temporal point parser first
    Temporal* temp = nullptr;
    {
        std::lock_guard<std::mutex> lk(meos_parse_mutex);
        temp = tgeompoint_in(wkt_string.c_str());
    }

    // If failed, try toggling POINT/Point case
    if (temp == nullptr)
    {
        std::string alt = wkt_string;
        if (auto pos = alt.find("Point("); pos != std::string::npos)
        {
            alt.replace(pos, 6, "POINT(");
            std::lock_guard<std::mutex> lk2(meos_parse_mutex);
            temp = tgeompoint_in(alt.c_str());
        }
        else if (auto pos2 = alt.find("POINT("); pos2 != std::string::npos)
        {
            alt.replace(pos2, 6, "Point(");
            temp = tgeompoint_in(alt.c_str());
        }
    }

    // Fall back to generic temporal geometry parser
    if (temp == nullptr)
    {
        std::lock_guard<std::mutex> lk3(meos_parse_mutex);
        temp = tgeometry_in(wkt_string.c_str());
    }

    if (temp == nullptr)
    {
        geometry = nullptr;
    }
    else
    {
        geometry = temp;
    }
}

Temporal* Meos::TemporalGeometry::getGeometry() const
{
    return geometry;
}

Meos::TemporalGeometry::~TemporalGeometry()
{
    // See note above about allocator mismatch.
    geometry = nullptr;
}

int Meos::TemporalGeometry::intersects(const TemporalGeometry& geom) const
{
    int result = eintersects_tgeo_tgeo((const Temporal*)this->geometry, (const Temporal*)geom.geometry);
    return result;
}

int Meos::TemporalGeometry::contains(const TemporalGeometry& geom) const
{
    int result = econtains_tgeo_tgeo((const Temporal*)this->geometry, (const Temporal*)geom.geometry);
    return result;
}

// StaticGeometry implementation
Meos::StaticGeometry::StaticGeometry(const std::string& wkt_string)
{
    ensureMeosInitialized();

    // Use geom_in to parse static WKT geometry (no temporal component)
    {
        std::lock_guard<std::mutex> lk(meos_parse_mutex);
        geometry = geom_in(wkt_string.c_str(), -1);
    }
}

Meos::StaticGeometry::StaticGeometry(const uint32_t& wkbType, const int8_t* wkbContent, const uint64_t& wkbContentSize)
{
    ensureMeosInitialized();
    /// Switch case dispatches wkbType
    switch (wkbType)
    {
        /// Polygon
        case 3: {
            /// Allocate space: We currently assume that the polygon consists of a single linear ring (so it contains no holes) -> this leads to 13 + wkbContentSize bytes being allocated
            const size_t contentSize = 13 + wkbContentSize;
            std::vector<std::byte> wkbBuffer(contentSize);
            /// Write the byte order (little endian)
            *reinterpret_cast<uint8_t*>(wkbBuffer.data()) = 1;
            /// wkbType
            *reinterpret_cast<uint32_t*>(wkbBuffer.data() + 1) = 3;
            /// Number of linear rings
            *reinterpret_cast<uint32_t*>(wkbBuffer.data() + 5) = 1;
            /// Number of points (divide wkbContent size by size of a point (16 bytes, 8 for x and 8 for y)
            *reinterpret_cast<uint32_t*>(wkbBuffer.data() + 9) = wkbContentSize / 16;
            /// Bytealign the point data
            std::memcpy(wkbBuffer.data() + 13, wkbContent, wkbContentSize);

            /// Use the ewkb constructor for geospatial types in meos
            {
                std::lock_guard<std::mutex> lk(meos_parse_mutex);
                geometry = geo_from_ewkb(reinterpret_cast<uint8_t*>(wkbBuffer.data()), contentSize, 4326);
                return;
            }
        }
        default: {
            INVARIANT(false, "WKB-Type {} not implemented yet", wkbType);
        }
    }
}

GSERIALIZED* Meos::StaticGeometry::getGeometry() const
{
    return geometry;
}

Meos::StaticGeometry::~StaticGeometry()
{
    // See note above about allocator mismatch.
    geometry = nullptr;
}

int Meos::TemporalGeometry::intersectsStatic(const StaticGeometry& static_geom) const
{
    // Use eintersects_tgeo_geo for temporal-static intersection
    int result = eintersects_tgeo_geo((const Temporal*)this->geometry, static_geom.getGeometry());

    return result;
}

int Meos::TemporalGeometry::aintersects(const TemporalGeometry& geom) const
{
    int result = aintersects_tgeo_tgeo((const Temporal*)this->geometry, (const Temporal*)geom.geometry);
    return result;
}

int Meos::TemporalGeometry::aintersectsStatic(const StaticGeometry& static_geom) const
{
    // Use aintersects_tgeo_geo for temporal-static intersection
    int result = aintersects_tgeo_geo((const Temporal*)this->geometry, static_geom.getGeometry());

    return result;
}

// called if static geometry is the first parameter
int Meos::StaticGeometry::containsTemporal(const TemporalGeometry& temporal_geom) const
{
    return econtains_geo_tgeo((const GSERIALIZED*)this->geometry, (const Temporal*)temporal_geom.getGeometry());
}

// called if temporal geometry is the first parameter
int Meos::TemporalGeometry::containsStatic(const StaticGeometry& static_geom) const
{
    return econtains_tgeo_geo((const Temporal*)this->geometry, (const GSERIALIZED*)static_geom.getGeometry());
}

// Constructor for creating a trajectory from multiple temporal instants
Meos::TemporalSequence::TemporalSequence(const std::vector<TemporalInstant*>& /*instants*/)
{
    // Ensure MEOS is initialized
    ensureMeosInitialized();

    sequence = nullptr;
    // TODO:call the aggregation function

    //TODO: with the result of the aggregation function, we can create a temporal sequence
}

Meos::TemporalSequence::~TemporalSequence()
{
    // Do not free; lifetime is managed by MEOS/PG memory context.
    sequence = nullptr;
}

double Meos::TemporalSequence::length(const TemporalInstant& /* instant */) const
{
    // Placeholder implementation
    // Using comment to avoid unused parameter warning
    return 0.0;
}

// Static wrapper functions for MEOS API
void* Meos::parseTemporalPoint(const std::string& trajStr)
{
    ensureMeosInitialized();

    if (trajStr.empty())
    {
        return nullptr;
    }

    // Clear any previous errors
    meos_errno_reset();

    Temporal* temp = tgeompoint_in(trajStr.c_str());
    if (!temp)
    {
        // Try with SRID prefix as fallback
        std::string sridStr = "SRID=4326;" + trajStr;
        temp = tgeompoint_in(sridStr.c_str());
    }

    return temp;
}

void Meos::freeTemporalObject(void* temporal)
{
    // Intentionally no-op; avoid allocator mismatch.
    (void)temporal;
}

void Meos::freeMeosPointer(void* ptr)
{
    // Intentionally no-op; avoid allocator mismatch.
    (void)ptr;
}

uint8_t* Meos::temporalToWKB(void* temporal, size_t& size)
{
    if (!temporal)
    {
        size = 0;
        return nullptr;
    }

    // Use extended WKB format (0x08)
    uint8_t* data = temporal_as_wkb((Temporal*)temporal, 0x08, &size);
    return data;
}

void Meos::ensureMeosInitialized()
{
    MEOS::ensureMeosInitialized();
}

int Meos::safe_edwithin_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs, double dist)
{
    std::lock_guard<std::mutex> lk(meos_exec_mutex);
    return edwithin_tgeo_geo(temp, gs, dist);
}

int Meos::safe_eintersects_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs)
{
    std::lock_guard<std::mutex> lk(meos_exec_mutex);
    return eintersects_tgeo_geo(temp, gs);
}

Temporal* Meos::safe_tgeo_at_stbox(const Temporal* temp, const STBox* box, bool border_inc)
{
    std::lock_guard<std::mutex> lk(meos_exec_mutex);
    return tgeo_at_stbox(temp, box, border_inc);
}

double Meos::safe_nad_tgeo_tgeo(const Temporal* temp1, const Temporal* temp2)
{
    std::lock_guard<std::mutex> lk(meos_exec_mutex);
    return nad_tgeo_tgeo(temp1, temp2);
}

// SpatioTemporalBox implementation
Meos::SpatioTemporalBox::SpatioTemporalBox(const std::string& wkt_string)
{
    // Ensure MEOS is initialized
    ensureMeosInitialized();
    // Use MEOS stbox_in function to parse the WKT string
    {
        std::lock_guard<std::mutex> lk(meos_parse_mutex);
        stbox_ptr = stbox_in(wkt_string.c_str());
        if (!stbox_ptr)
        {
            // Attempt to convert legacy STBOX((x,y,t),(x2,y2,t2)) into STBOX XT(((x,y),(x2,y2)),[t,t2])
            std::string sridPrefix;
            std::string core = wkt_string;
            if (auto semi = core.find(';'); semi != std::string::npos)
            {
                sridPrefix = core.substr(0, semi + 1); // keep trailing ';'
                core = core.substr(semi + 1);
            }
            auto start = core.find("STBOX((");
            auto end = core.rfind(")");
            if (start != std::string::npos && end != std::string::npos && end > start + 8)
            {
                std::string inner = core.substr(start + 7, end - (start + 7)); // after 'STBOX('
                // Expect inner like: (x,y,t),(x2,y2,t2)
                // Remove possible outer parentheses
                if (!inner.empty() && inner.front() == '(' && inner.back() == ')')
                {
                    inner = inner.substr(1, inner.size() - 2);
                }
                auto mid = inner.find("),(");
                if (mid != std::string::npos)
                {
                    auto first = inner.substr(0, mid);
                    auto second = inner.substr(mid + 3);
                    auto trim = [](std::string& s)
                    {
                        while (!s.empty() && (s.front() == ' ' || s.front() == '\t'))
                            s.erase(s.begin());
                        while (!s.empty() && (s.back() == ' ' || s.back() == '\t'))
                            s.pop_back();
                        if (!s.empty() && s.front() == '(')
                            s.erase(s.begin());
                        if (!s.empty() && s.back() == ')')
                            s.pop_back();
                    };
                    trim(first);
                    trim(second);
                    auto split3 = [](const std::string& s)
                    {
                        std::vector<std::string> out;
                        out.reserve(3);
                        size_t p = 0;
                        size_t c1 = s.find(',');
                        if (c1 == std::string::npos)
                            return out;
                        size_t c2 = s.find(',', c1 + 1);
                        if (c2 == std::string::npos)
                            return out;
                        out.push_back(s.substr(0, c1));
                        out.push_back(s.substr(c1 + 1, c2 - (c1 + 1)));
                        out.push_back(s.substr(c2 + 1));
                        return out;
                    };
                    auto a = split3(first);
                    auto b = split3(second);
                    if (a.size() == 3 && b.size() == 3)
                    {
                        auto trimSpaces = [](std::string& s)
                        {
                            while (!s.empty() && (s.front() == ' ' || s.front() == '\t'))
                                s.erase(s.begin());
                            while (!s.empty() && (s.back() == ' ' || s.back() == '\t'))
                                s.pop_back();
                        };
                        for (auto* v : {&a[0], &a[1], &a[2], &b[0], &b[1], &b[2]})
                            trimSpaces(*v);
                        std::string xt = sridPrefix + "STBOX XT(((" + a[0] + "," + a[1] + "),(" + b[0] + "," + b[1] + ")), [" + a[2] + ", "
                            + b[2] + "])";
                        stbox_ptr = stbox_in(xt.c_str());
                    }
                }
            }
        }
    }
}

Meos::SpatioTemporalBox::SpatioTemporalBox(double minX, double maxX, double minY, double maxY, long long minTs, long long maxTs, int srid)
{
    ensureMeosInitialized();
    {
        std::lock_guard<std::mutex> lk(meos_parse_mutex);
        const TimestampTz convertedMinTs = convertEpochToTimestampTz(minTs);
        const TimestampTz convertedMaxTs = convertEpochToTimestampTz(maxTs);
        /// Create time span out of our ts (for now we assume they are inclusive)
        const Span* timespan = tstzspan_make(convertedMinTs, convertedMaxTs, true, true);
        /// Create STBox out of the given boundaries and the span
        /// For now, we default geodetic to false
        stbox_ptr = stbox_make(true, false, false, srid, minX, maxX, minY, maxY, 0, 0, timespan);
    }
}

Meos::SpatioTemporalBox::~SpatioTemporalBox()
{
    // Do not free; managed by MEOS.
    stbox_ptr = nullptr;
}

STBox* Meos::SpatioTemporalBox::getBox() const
{
    return static_cast<STBox*>(stbox_ptr);
}

Meos::TemporalHolder::TemporalHolder(Temporal* temporalPtr) : temporal(temporalPtr)
{
}

Meos::TemporalHolder::~TemporalHolder()
{
    // Do not free; managed by MEOS.
    temporal = nullptr;
}

Temporal* Meos::TemporalHolder::get() const
{
    return temporal;
}


} // namespace MEOS
