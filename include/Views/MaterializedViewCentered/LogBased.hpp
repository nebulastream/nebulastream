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

#ifndef NES_LOGBASED_HPP
#define NES_LOGBASED_HPP

/**
 * Log-based materialized view.
 * Consists of a log data structure and a table data structure.
 * New element are inserted at the beginning of the log.
 *
 * The following calls are supported:
 * 1) Insert: insert into log and table
 * 2) Scan: iterate over the log using the iterator interface
 * 3) New Window: reset the log and table content
 *
 * We use the following call semantic (MV-centered):
 * 1) On new stream tuple:
 *  δx ← Maintenance-Query(Input-Tuples)
 *  x ← MV.get()
 *  MV.write(Window-Aggregate(δx, x))
 * 2) On ad how query:
 *  result ← Ad-Hoc-Query(MV.scan())
 * 3) On window end:
 *  MV.clear()
 */

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;

// Number of log entries per log buffer
constexpr int32_t logsPerBuffer  = 32;

/**
 * Log entry to store tuples in key and value format
 * @tparam K Key
 * @tparam V Value
 */
template <typename K, typename V>
struct LogEntry {
    // key of the log entry
    K key;

    // value of the log entry
    V value;

    // pointer to older version of this log entry
    LogEntry* previous;

    // entry is valid from this time
    //long int validFrom;
    TimePoint validFrom;

    // entry is valid to this time
    //long int validTo = 0; // default
    TimePoint validTo = TimePoint::max();

    // entry is valid
    //bool isValid = true;  // default
};

/**
 * Log buffer which stores multiple (#logsPerBuffer) log entries
 * @tparam K Key
 * @tparam V Value
 */
template <typename K, typename V>
struct LogBuffer {
    // array of log entries
    std::array<LogEntry<K,V>, logsPerBuffer> entries;

    // pointer to next log buffer in log
    LogBuffer<K,V> *next;
};

/**
 * Log-based materialized view with centered MV-centered call semantic
 *
 * @tparam K Key type
 * @tparam V Value record type
 * @tparam I Input record type
 */
template <typename K = uint64_t, typename V = AMRecord, typename I = EventRecord>
class LogBasedMV: public MaterializedView<K, I> {
public:

    /**
     * Default constructor
     */
    LogBasedMV() = default;

    /**
     * Get log entry for key
     *
     * @param key
     * @return
     */
    LogEntry<K,V>* get(K key) {
        if (index.count(key) == 1) {
            return index[key];
        } else {
            return nullptr;
        }
    }

    /**
     * Inserts a new record to log and index
     * New tuple are inserted at the beginning of the log.
     * Log entries should be treated as read-only.
     *
     * Assumes one-threaded insert
     *
     * @param key
     * @param event
     */
    /*void insert(K key, V record) {
        auto timestamp = Clock::now();

        // get the insert pos
        const uint64_t insertPos = logSize.load();

        // calculate insert pos in current buffer
        const uint64_t insertPosBuffer =  insertPos & (logsPerBuffer - 1);

        // update log buffer head, if needed
        if (!head) { // case: first record in log

            // create new log buffer and use as head
            head = new LogBuffer<K, V>();

        } else if (insertPosBuffer == 0) { // case: current head is full

            // store current head in tmp variable
            auto tmp = head;

            // create new log buffer and store as head
            head = new LogBuffer<K, V>();

            // set new buffer to previous head
            head->next = tmp;
        }

        // create a new log entry and insert into top of the log and into the index
        if (index.count(key) == 1) { // case: key in present in index

            // update old entry valid
            oldEntry->validTo = timestamp;

            // insert entry at log position
            head->entries.at(insertPosBuffer) = {key,      // key
                                                 record, // create updated record
                                                 oldEntry, // pointer to old entry
                                                 timestamp // valid from
            };

            // update index to point to the newest entry
            index[key] = &(head->entries.at(insertPosBuffer));

        } else { // case: key is not present in index

            // insert entry at log positiong
            head->entries.at(insertPosBuffer) = {key,      // key
                                                 AMRecord(event), // create record
                                                 nullptr,  // pointer to old entry
                                                 timestamp // valid from
            };

            // update index
            index.insert({key, &(head->entries.at(insertPosBuffer))});
        }

        // new log size
        logSize++;
    };*/

    /**
     * Inserts a new record to log and index
     * New tuple are inserted at the beginning of the log.
     * Log entries should be treated as read-only.
     *
     * Assumes one-threaded insert
     *
     * @param key
     * @param event
     */
    void insert(K key, I event) {
        auto timestamp = Clock::now();

        // get the insert pos
        const uint64_t insertPos = logSize.load();

        // calculate insert pos in current buffer
        const uint64_t insertPosBuffer =  insertPos & (logsPerBuffer - 1);

        // update log buffer head, if needed
        if (!head) { // case: first record in log

            // create new log buffer and use as head
            head = new LogBuffer<K, V>();

        } else if (insertPosBuffer == 0) { // case: current head is full

            // store current head in tmp variable
            auto tmp = head;

            // create new log buffer and store as head
            head = new LogBuffer<K, V>();

            // set new buffer to previous head
            head->next = tmp;
        }

        // create a new log entry and insert into top of the log and into the index
        if (index.count(key) == 1) { // case: key in present in index

            // get pointer to current entry with key
            auto oldEntry = index[key];

            // update old entry valid
            oldEntry->validTo = timestamp;

            // insert entry at log position
            head->entries.at(insertPosBuffer) = {key,      // key
                                                 AMRecord(&(oldEntry->value), event), // create updated record
                                                 oldEntry, // pointer to old entry
                                                 timestamp // valid from
                                                 };

            // update index to point to the newest entry
            index[key] = &(head->entries.at(insertPosBuffer));

        } else { // case: key is not present in index

            // insert entry at log positiong
            head->entries.at(insertPosBuffer) = {key,      // key
                                                 AMRecord(event), // create record
                                                 nullptr,  // pointer to old entry
                                                 timestamp // valid from
                                                 };

            // update index
            index.insert({key, &(head->entries.at(insertPosBuffer))});
        }

        // new log size
        logSize++;
    };

    /**
     * Trigger a new window
     */
    void newWindow(){
        // clear log
        LogBuffer<K,V> *temp;
        auto cnt = 0;
        while (head != nullptr) {
            temp = head->next;
            delete head;
            head = temp;
            cnt++;
        }
        NES_INFO("LogBased::newWindow: cleared buffers: " << cnt << " log entries: " << logSize << " index entries: " << index.size());
        head = nullptr;
        logSize = 0;

        // clear index
        index.clear();
    }

    /**
     * (Forward-)Iterator to enable the scan of the materialized view
     *
     * Source:
     * https://www.internalpointers.com/post/writing-custom-iterators-modern-cpp
     * https://medium.com/geekculture/iterator-design-pattern-in-c-42caec84bfc
     */
    struct Iterator {
        // Iterator tags
        using iterator_category = std::forward_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = LogEntry<K,V>;
        using pointer           = LogEntry<K,V>*;  // or also value_type*
        using reference         = LogEntry<K,V>&;  // or also value_type&
        using bufferPtr         = LogBuffer<K,V>*;

        // default constructor
        Iterator() noexcept : currentBuffer(nullptr), size(0) {};

        // constructor that takes a log buffer and size the current log entry number
        Iterator(const bufferPtr &buffer, const uint64_t size, TimePoint time) noexcept : currentBuffer(buffer),
                                                                            size(size),
                                                                            numFullBuffer(size / logsPerBuffer),
                                                                            elemInFullBuffers(numFullBuffer * logsPerBuffer),
                                                                            elemHead(size - elemInFullBuffers),
                                                                            time(time) {};

        /**
         * Prefix incrementing
         *
         * Iterate through log.
         * Head contians the only buffer with variable element number.
         * Other buffers are guaranteed to store #logsPerBuffer elements.
         * @return Iterator
         */
        Iterator &operator++() noexcept {
            if (currentBuffer != nullptr) { // case: current buffer is not null

                uint64_t localIndex;
                do {
                    bool endOfBuffer;
                    if (globalIndex + 1 <= elemHead) { // case: current index in head buffer
                        endOfBuffer = (globalIndex + 1) % (size - elemInFullBuffers) == 0;
                    } else { // case: current index not in head buffer
                        endOfBuffer = (globalIndex + 1 - elemHead) % logsPerBuffer == 0;
                    }

                    if (endOfBuffer) {  // reached end of a buffer
                        if (!currentBuffer->next) { // case: reached end of log
                            // currentBuffer is null
                            currentBuffer = nullptr;

                        } else { // case: next buffer exists
                            // iterate the next linked buffer
                            currentBuffer = currentBuffer->next;

                        }
                    }

                    // iterate the global index
                    globalIndex++;

                    // Get local index
                    if (globalIndex < elemHead) {  // case: global index is in head
                        localIndex = globalIndex;
                    } else { // case: global index is in one of the full buffers
                        localIndex = (globalIndex - elemHead) % logsPerBuffer;
                    }
                } while (currentBuffer != nullptr &&
                        (this->currentBuffer->entries[localIndex].validFrom > time ||
                        time >= this->currentBuffer->entries[localIndex].validTo ));

            }
            return *this;
        };

        /**
         * Postfix incrementing
         * @return Iterator
         */
        Iterator operator++(int) noexcept {
            // make a copy of the iterator
            Iterator temp = *this;

            // increment
            ++*this;

            // return the copy before increment
            return temp;
        };

        /**
         * Compare operator for the iterator
         * @param other
         * @return
         */
        bool operator!=(const Iterator &other) const noexcept {
            // unequal if both are null
            if (!(this->currentBuffer) && !(other.currentBuffer)){
                return false;
            }

            if (this->currentBuffer != other.currentBuffer) { // case: buffer are unequal
                return true;
            } else if (this->globalIndex != other.globalIndex) { // case: index are unequal
                return true;
            }
            return false;
        };

        /**
         * Function operator for the iterator
         * Return the current entry from the buffer
         * @return
         */
        LogEntry<K, V> operator*() const noexcept {
            // the local index in the current buffer
            uint64_t localIndex;

            if (globalIndex < elemHead) {  // case: global index is in head
                localIndex = globalIndex;
            } else { // case: global index is in one of the full buffers
                localIndex = (globalIndex - elemHead) % logsPerBuffer;
            }

            return this->currentBuffer->entries[localIndex];
        };

    private:
        // hold the current buffer
        LogBuffer<K,V> *currentBuffer = nullptr;

        // current size of log
        const uint64_t size = 0;

        // current position in the whole log
        uint64_t globalIndex = 0;

        // number of full buffers
        const uint64_t numFullBuffer = 0;

        // number of elements in all full buffers (non-head buffers)
        const uint64_t elemInFullBuffers = 0;

        // number of elements head buffer
        const uint64_t elemHead = 0;

        // snapshot time of iterator
        TimePoint time;
    };

    /**
     * Return the begin iterator
     * @return
     */
    Iterator begin() const noexcept {
        return Iterator(this->head,
                        this->logSize.load(),
                        Clock::now());
    }

    /**
     * retrun the end iterator
     * @return
     */
    Iterator end() const noexcept {
        return Iterator();
    }

    /**
     * Return the size of the materialized view which is the size of the index
     * @return
     */
    uint64_t size() {
        //return index.size();
        return logSize.load();
    }

    /**
     * Return the name of the materialized view
     * @return
     */
    std::string name() {
        return "Log-based Materialized View-centered";
    }

    /**
     * Print the log content
     */
    void printLog(){
        for (auto it = begin(); it != end(); it++) {
            NES_INFO("" << (* it).key);
        }
    }

private:
    // index as map from key to log entry
    std::unordered_map<K, LogEntry<K,V>* > index;

    // head of log data structure
    LogBuffer<K,V> *head = nullptr;

    // the current log size. It is used as point of synchronisation
    std::atomic<uint64_t> logSize = 0;
};

#endif //NES_LOGBASED_HPP
