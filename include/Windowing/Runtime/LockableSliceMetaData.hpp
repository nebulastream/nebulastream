#ifndef NES_INCLUDE_WINDOWING_RUNTIME_LOCKSLICEMETADATA_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_LOCKSLICEMETADATA_HPP_

#include <cstdint>
#include <mutex>

namespace NES::Windowing {

/**
* SliceMetaData stores the meta data of a slice to identify if a record can be assigned to a particular slice.
*/
class LockableSliceMetaData {
  public:
    /**
     * @brief Construct a new slice meta data object.
     * @param startTs
     * @param endTs
     */
    LockableSliceMetaData(uint64_t startTs, uint64_t endTs);

    /**
    * @brief The start timestamp of this slice
    */
    uint64_t getStartTs();

    /**
     * @brief The end timestamp of this slice
     */
    uint64_t getEndTs();

    /**
     * @brief method to get the number of tuples per slice
     * @return number of tuples per slice
     */
    uint64_t getRecordsPerSlice();

    /**
     * @brief method to increment the number of tuples per slice by one
     */
    void incrementRecordsPerSlice();

    /**
     * @brief method to increment the number of tuples per slice by value
     * @param value
     */
    void incrementRecordsPerSliceByValue(uint64_t value);

  private:
    std::recursive_mutex mutex;
    uint64_t startTs;
    uint64_t endTs;
    uint64_t recordsPerSlice;
};
}

#endif//NES_INCLUDE_WINDOWING_RUNTIME_LOCKSLICEMETADATA_HPP_
