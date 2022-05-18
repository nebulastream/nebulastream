/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_MATRIXRECORDS_HPP
#define NES_MATRIXRECORDS_HPP

#include <algorithm>
#include <array>

/**
 * Column Store Storage Layout for Analytics Matrix
 * |.......................................................IntAggs...................................................|..
 * |..............NoFilter..............|.................LocalFilter.............|...........NonLocalFilter.........|..
 * |......Week.......|.......Day........|......Week...
 * |Basic|Basic|.....|Basic|Basic...
 * Basic = Min, Max, Sum, Count for Int Aggregates, and Min, Max, Sum for Long
 *
 * Basic types are duplicated by AGG_DUPLICATION_FACTOR
 */

constexpr int16_t AGG_DUPLICATION_FACTOR = 13;

constexpr int16_t DAILY_INT_OFFSET = 4 * AGG_DUPLICATION_FACTOR;
constexpr int16_t DAILY_LONG_OFFSET = 3 * AGG_DUPLICATION_FACTOR;
// Integer offsets (duration and count)
constexpr int16_t DURATION_SUM_OFFSET = 0;
constexpr int16_t DURATION_MAX_OFFSET = 1;
constexpr int16_t DURATION_MIN_OFFSET = 2;
constexpr int16_t CALLS_COUNT_OFFSET = 3;
// Long offsets (cost)
constexpr int16_t COST_SUM_OFFSET = 0;
constexpr int16_t COST_MAX_OFFSET = 1;
constexpr int16_t COST_MIN_OFFSET = 2;

constexpr int16_t NO_FILTER_OFFSET = 0;
constexpr int16_t WEEKLY_OFFSET = 0;

constexpr int16_t NUM_WINDOWS = 2;
constexpr int16_t NUM_FILTER = 3;

constexpr int16_t NUM_INT_AGGS_PER_FILTER  = AGG_DUPLICATION_FACTOR * 4 * NUM_WINDOWS;
constexpr int16_t LOCAL_INT_OFFSET         = NUM_INT_AGGS_PER_FILTER;
constexpr int16_t NON_LOCAL_INT_OFFSET     = LOCAL_INT_OFFSET + NUM_INT_AGGS_PER_FILTER;
constexpr int16_t NUM_LONG_AGGS_PER_FILTER = AGG_DUPLICATION_FACTOR * 3 * NUM_WINDOWS;
constexpr int16_t LOCAL_COST_OFFSET        = NUM_LONG_AGGS_PER_FILTER;
constexpr int16_t NON_LOCAL_COST_OFFSET    = LOCAL_COST_OFFSET + NUM_LONG_AGGS_PER_FILTER;

constexpr int16_t INT_AGGS  = NUM_FILTER * NUM_WINDOWS * 4 /* Basics */ * AGG_DUPLICATION_FACTOR;
constexpr int16_t LONG_AGGS = NUM_FILTER * NUM_WINDOWS * 3 /* Basics */ * AGG_DUPLICATION_FACTOR;


struct __attribute__((packed)) EventRecord {
    int32_t mv$id;
    float mv$cost;
    int32_t mv$duration;
    int64_t mv$timestamp;
    bool mv$isLongDistance;

    bool operator==(const EventRecord& rhs) const {
        return ((this->mv$id == rhs.mv$id) &&
        (this->mv$cost == rhs.mv$cost) &&
        (this->mv$duration == rhs.mv$duration) &&
        (this->mv$timestamp == rhs.mv$timestamp) &&
        (this->mv$isLongDistance == rhs.mv$isLongDistance));
    }
};

#define MV_HT_APPEND_ONLY 1
#ifdef MV_HT_APPEND_ONLY

uint64_t microseconds_since_epoch() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/**
 * Use the two-phase locking protocol
 */
struct AMRecord {

    std::array<int32_t, INT_AGGS> intAggs = {};
    std::array<int64_t, LONG_AGGS> longAggs = {};

    uint64_t txn_id = 0;
    uint64_t begin_ts = 0;
    uint64_t end_ts = 0;
    uint64_t read_cnt = 0; // number of active tx that have read the record
    std::shared_ptr<AMRecord> nextVersion;

    AMRecord() = default;

    // Create new AMRecord without old version
    AMRecord(EventRecord record) {
        update(record, true);
    }

    /**
     * Create a new version to append at the end
     */
    AMRecord(AMRecord *version, EventRecord record) {
        this->intAggs  = version->intAggs;
        this->longAggs = version->longAggs;
        this->txn_id = version->txn_id;
        this->begin_ts = version->begin_ts;
        this->end_ts = version->end_ts;
        this->read_cnt = version->read_cnt;
        this->nextVersion = version->nextVersion;

        this->update(record, false);
    }

    AMRecord(std::shared_ptr<AMRecord> version, EventRecord record) {
        this->intAggs  = version->intAggs;
        this->longAggs = version->longAggs;
        this->txn_id = version->txn_id;
        this->begin_ts = version->begin_ts;
        this->end_ts = version->end_ts;
        this->read_cnt = version->read_cnt;
        this->nextVersion = version->nextVersion;

        this->update(record, false);
    }

    void update(EventRecord record, bool firstUpdate = false) {
        int64_t cost = record.mv$cost * 100; // convert to long

        // Update non-filter fields
        updateAggregates(record.mv$duration, NO_FILTER_OFFSET, firstUpdate);
        updateAggregates(cost, NO_FILTER_OFFSET, firstUpdate);

        // Update filter fields
        if (record.mv$isLongDistance) {
            updateAggregates(record.mv$duration, LOCAL_INT_OFFSET, firstUpdate);
            updateAggregates(cost, LOCAL_COST_OFFSET, firstUpdate);
        } else {
            updateAggregates(record.mv$duration, LOCAL_INT_OFFSET, firstUpdate);
            updateAggregates(cost, LOCAL_COST_OFFSET, firstUpdate);
        }
    }

    bool operator==(const AMRecord& rhs) const {
        return ((this->intAggs == rhs.intAggs) && (this->longAggs== rhs.longAggs));
    }

private:
    void updateAggregates(int32_t duration, int filterOffset, bool firstUpdate) {
        for (int i = filterOffset; i < NUM_INT_AGGS_PER_FILTER + filterOffset; i += 4) {
            intAggs[i + DURATION_SUM_OFFSET] += duration;
            intAggs[i + DURATION_MAX_OFFSET] = std::max(intAggs[i + DURATION_MAX_OFFSET], duration);
            if (firstUpdate) {
                intAggs[i + DURATION_MIN_OFFSET] = duration;
            } else {
                intAggs[i + DURATION_MIN_OFFSET] = std::min(intAggs[i + DURATION_MIN_OFFSET], duration);
            }
            intAggs[i + CALLS_COUNT_OFFSET]++;
        }
    }

    void updateAggregates(int64_t cost, int filterOffset, bool firstUpdate) {
        for (int i = filterOffset; i < NUM_LONG_AGGS_PER_FILTER + filterOffset; i += 3) {
            longAggs[i + COST_SUM_OFFSET] += cost;
            if (firstUpdate) {
                longAggs[i + COST_MAX_OFFSET] = cost;
            } else {
                longAggs[i + COST_MAX_OFFSET] = std::max(longAggs[i + DURATION_MAX_OFFSET], cost);
            }
            longAggs[i + COST_MIN_OFFSET] = std::min(longAggs[i + DURATION_MIN_OFFSET], cost);
        }
    }
};

#else

struct AMRecord {

    AMRecord(EventRecord record) {
        update(record, true);
    }

    void update(EventRecord record, bool firstUpdate = false) {
        int64_t cost = record.mv$cost * 100; // convert to long

        // Update non-filter fields
        updateAggregates(record.mv$duration, NO_FILTER_OFFSET, firstUpdate);
        updateAggregates(cost, NO_FILTER_OFFSET, firstUpdate);

        // Update filter fields
        if (record.mv$isLongDistance) {
            updateAggregates(record.mv$duration, LOCAL_INT_OFFSET, firstUpdate);
            updateAggregates(cost, LOCAL_COST_OFFSET, firstUpdate);
        } else {
            updateAggregates(record.mv$duration, LOCAL_INT_OFFSET, firstUpdate);
            updateAggregates(cost, LOCAL_COST_OFFSET, firstUpdate);
        }
    }

    bool operator==(const AMRecord& rhs) const {
        return ((this->intAggs == rhs.intAggs) && (this->longAggs== rhs.longAggs));
    }

    std::array<int32_t, INT_AGGS> intAggs = {};
    std::array<int64_t, LONG_AGGS> longAggs = {};

private:
    void updateAggregates(int32_t duration, int filterOffset, bool firstUpdate) {
        for (int i = filterOffset; i < NUM_INT_AGGS_PER_FILTER + filterOffset; i += 4) {
            intAggs[i + DURATION_SUM_OFFSET] += duration;
            intAggs[i + DURATION_MAX_OFFSET] = std::max(intAggs[i + DURATION_MAX_OFFSET], duration);
            if (firstUpdate) {
                intAggs[i + DURATION_MIN_OFFSET] = duration;
            } else {
                intAggs[i + DURATION_MIN_OFFSET] = std::min(intAggs[i + DURATION_MIN_OFFSET], duration);
            }
            intAggs[i + CALLS_COUNT_OFFSET]++;
        }
    }

    void updateAggregates(int64_t cost, int filterOffset, bool firstUpdate) {
        for (int i = filterOffset; i < NUM_LONG_AGGS_PER_FILTER + filterOffset; i += 3) {
            longAggs[i + COST_SUM_OFFSET] += cost;
            if (firstUpdate) {
                longAggs[i + COST_MAX_OFFSET] = cost;
            } else {
                longAggs[i + COST_MAX_OFFSET] = std::max(longAggs[i + DURATION_MAX_OFFSET], cost);
            }
            longAggs[i + COST_MIN_OFFSET] = std::min(longAggs[i + DURATION_MIN_OFFSET], cost);
        }
    }
};

#endif // MV_HT_APPEND_ONLY

#endif //NES_MATRIXRECORDS_HPP
