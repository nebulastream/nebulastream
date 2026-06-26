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

#ifndef NES_PLUGINS_MEOS_HPP
#define NES_PLUGINS_MEOS_HPP

#include <string>
#include <vector>

extern "C" {
    #include <meos.h>
    #include <meos_geo.h>
}

namespace MEOS {

class Meos {
  public:
    /**
     * @brief Initialize MEOS library
     */
    Meos();

    /**
     * @brief Finalize MEOS library, free the timezone cache
     */
    ~Meos();

    class SpatioTemporalBox {
    public:
        /**
         * @brief Create SpatioTemporal from WKT string
         * @param wkt_string String in format "SRID=4326;SpatioTemporal X((3.5, 50.5),(4.5, 51.5))"
         */
        explicit SpatioTemporalBox(const std::string& wkt_string);

        /// Constructor to create a 2-dimensional STBox out of c-native input parameters
        explicit SpatioTemporalBox(double minX, double maxX, double minY, double maxY, long long minTs, long long maxTs, int srid=4326);
        ~SpatioTemporalBox();

        // Non-copyable, movable to avoid double-free of MEOS-managed memory
        SpatioTemporalBox(const SpatioTemporalBox&) = delete;
        SpatioTemporalBox& operator=(const SpatioTemporalBox&) = delete;
        SpatioTemporalBox(SpatioTemporalBox&& other) noexcept : stbox_ptr(other.stbox_ptr) { other.stbox_ptr = nullptr; }
        SpatioTemporalBox& operator=(SpatioTemporalBox&& other) noexcept {
            if (this != &other) { stbox_ptr = other.stbox_ptr; other.stbox_ptr = nullptr; }
            return *this;
        }

        STBox* getBox() const;


    private:
        void* stbox_ptr;
    };

    class TemporalInstant {
    public:
        explicit TemporalInstant(double lon, double lat, long long ts, int srid=4326);
        ~TemporalInstant();

        TemporalInstant(const TemporalInstant&) = delete;
        TemporalInstant& operator=(const TemporalInstant&) = delete;
        TemporalInstant(TemporalInstant&& other) noexcept : instant(other.instant) { other.instant = nullptr; }
        TemporalInstant& operator=(TemporalInstant&& other) noexcept { if (this!=&other){ instant = other.instant; other.instant=nullptr;} return *this; }

        bool intersects(const TemporalInstant& point) const;
        Temporal* getGeometry();

    private:
        Temporal* instant;
    };

    class StaticGeometry;
    class TemporalGeometry;
    class StaticGeometry {
    public:
        explicit StaticGeometry(const std::string& wkt_string);
        ~StaticGeometry();

        StaticGeometry(const StaticGeometry&) = delete;
        StaticGeometry& operator=(const StaticGeometry&) = delete;
        StaticGeometry(StaticGeometry&& other) noexcept : geometry(other.geometry) { other.geometry = nullptr; }
        StaticGeometry& operator=(StaticGeometry&& other) noexcept { if (this!=&other){ geometry = other.geometry; other.geometry=nullptr;} return *this; }

        GSERIALIZED* getGeometry() const;

        int containsTemporal(const TemporalGeometry& temporal_geom) const;
        // int coversTemporal(const TemporalGeometry& temporal_geom) const;

    private:
        GSERIALIZED* geometry;
    };

    class TemporalGeometry {
    public:
        explicit TemporalGeometry(const std::string& wkt_string);
        ~TemporalGeometry();

        TemporalGeometry(const TemporalGeometry&) = delete;
        TemporalGeometry& operator=(const TemporalGeometry&) = delete;
        TemporalGeometry(TemporalGeometry&& other) noexcept : geometry(other.geometry) { other.geometry = nullptr; }
        TemporalGeometry& operator=(TemporalGeometry&& other) noexcept { if (this!=&other){ geometry = other.geometry; other.geometry=nullptr;} return *this; }

        Temporal* getGeometry() const;

        int intersects(const TemporalGeometry& geom) const;
        int intersectsStatic(const StaticGeometry& static_geom) const;
        
        int aintersects(const TemporalGeometry& geom) const;
        int aintersectsStatic(const StaticGeometry& static_geom) const;

        int contains(const TemporalGeometry& geom) const;
        int containsStatic(const StaticGeometry& static_geom) const;

        // int covers(const TemporalGeometry& geom) const;
        // int coversStatic(const StaticGeometry& static_geom) const;

        // int disjoint(const TemporalGeometry& geom) const;
        // int disjointStatic(const StaticGeometry& static_geom) const;
        
        // int dwithin(const TemporalGeometry& geom, double dist) const;
        // int dwithinStatic(const StaticGeometry& static_geom, double dist) const;

        // int touches(const TemporalGeometry& geom) const;
        // int touchesStatic(const StaticGeometry& static_geom) const;

    private:
        Temporal* geometry;
    };
    
    class TemporalSequence {
    public:
        // Constructor for creating trajectory from multiple points
        explicit TemporalSequence(const std::vector<TemporalInstant*>& instants);
        
        // Constructor for creating trajectory from coordinate arrays
        TemporalSequence(const std::vector<double>& longitudes, 
                        const std::vector<double>& latitudes, 
                        const std::vector<long long>& timestamps, 
                        int srid = 4326);
        
        ~TemporalSequence();

        TemporalSequence(const TemporalSequence&) = delete;
        TemporalSequence& operator=(const TemporalSequence&) = delete;
        TemporalSequence(TemporalSequence&& other) noexcept : sequence(other.sequence) { other.sequence = nullptr; }
        TemporalSequence& operator=(TemporalSequence&& other) noexcept { if (this!=&other){ sequence = other.sequence; other.sequence=nullptr;} return *this; }

        
        // bool intersects(const TemporalInstant& point) const;
        // double distance(const TemporalInstant& point) const;
        double length(const TemporalInstant& instant) const;

    private:
        Temporal* sequence;
    };


    class TemporalHolder {
    public:
        explicit TemporalHolder(Temporal* temporalPtr);
        ~TemporalHolder();

        TemporalHolder(const TemporalHolder&) = delete;
        TemporalHolder& operator=(const TemporalHolder&) = delete;
        TemporalHolder(TemporalHolder&& other) noexcept : temporal(other.temporal) { other.temporal = nullptr; }
        TemporalHolder& operator=(TemporalHolder&& other) noexcept { if (this!=&other){ temporal = other.temporal; other.temporal=nullptr;} return *this; }

        Temporal* get() const;

    private:
        Temporal* temporal;
    };


    // Format a UTC timestamp string from seconds since epoch
    static std::string convertSecondsToTimestamp(long long seconds);

    // Heuristically interpret an epoch-like value that may be in seconds,
    // milliseconds, microseconds, or nanoseconds, and format as UTC.
    // Common thresholds: 10 digits (seconds), 13 (ms), 16 (us), 19 (ns).
    static std::string convertEpochToTimestamp(unsigned long long epochLike);

    /// Converts epoch-like value that may be in  milliseconds, microseconds, or nanoseconds to the
    /// Meos-compatible version of the timestamp (microseconds since the PostgreSQL epoch, 2000-01-01 00:00:00 UTC)
    /// This does not require a int -> string conversion by avoiding the creation of a human-readable timestamp thus saving parsing time.
    /// Common thresholds: 10 digits (seconds), 13 (ms), 16 (us), 19 (ns)
    static TimestampTz convertEpochToTimestampTz(unsigned long long epochLike);

    // Thread-safe wrappers around selected MEOS functions to avoid internal races
    static int safe_edwithin_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs, double dist);
    static int safe_eintersects_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs);
    static Temporal* safe_tgeo_at_stbox(const Temporal* temp, const STBox* box, bool border_inc);
    static double safe_nad_tgeo_tgeo(const Temporal* temp1, const Temporal* temp2);

    /**
     * @brief Parse a temporal point string into a MEOS Temporal object
     * @param trajStr String representation of temporal point (e.g., "{Point(1.0 2.0)@2023-01-01 00:00:00}")
     * @return Void pointer to Temporal object, nullptr on failure. Caller must free with freeTemporalObject()
     */
    static void* parseTemporalPoint(const std::string& trajStr);
    
    /**
     * @brief Free a MEOS Temporal object
     * @param temporal Pointer to Temporal object to free
     */
    static void freeTemporalObject(void* temporal);
    
    /**
     * @brief Convert a Temporal object to WKB format
     * @param temporal Pointer to Temporal object
     * @param size Output parameter for the size of the WKB data
     * @return Pointer to WKB data, nullptr on failure. Caller should release it via freeMeosPointer().
     */
    static uint8_t* temporalToWKB(void* temporal, size_t& size);

    /**
     * @brief Release a pointer returned by MEOS APIs (e.g., temporal_as_wkb / temporal_as_hexwkb).
     *
     * MEOS originates from PostgreSQL/MobilityDB and may use different allocators depending on build
     * configuration. To avoid crashes from allocator mismatches, this is a no-op by default.
     */
    static void freeMeosPointer(void* ptr);
    
    /**
     * @brief Ensure MEOS is initialized
     */
    static void ensureMeosInitialized();
    
    bool finalized=false;

};

}
#endif // NES_PLUGINS_MEOS_HPP
