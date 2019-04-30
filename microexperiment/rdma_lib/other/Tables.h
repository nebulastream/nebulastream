//
// Created by Toso, Lorenzo on 2019-01-25.
//

#pragma once


#include <cstddef>
#include <vector>
#include <array>
#include "JoinParameters.h"

typedef std::array<char, B_PAYLOAD_SIZE> BPayload;
typedef std::array<char, P_PAYLOAD_SIZE> PPayload;



struct BuildTuple {
    size_t key;
    BPayload payload;
};
struct ProbeTuple {
    size_t key;
    PPayload payload;
};


template <class T>
class AbstractTable {
public:
    std::vector<size_t> keys;
    std::vector<T> payloads;

    AbstractTable() = default;

    explicit AbstractTable(size_t size){
        resize(size);
    }

    size_t size() const {
        return keys.size();
    }

    void clear() {
        keys.clear();
        payloads.clear();
    }

    void resize(size_t new_size) {
        keys.resize(new_size);
        payloads.resize(new_size);
    }
    void reserve(size_t new_size) {
        keys.reserve(new_size);
        payloads.reserve(new_size);
    }
    void set(size_t index, size_t key, const T & payload) {
        keys[index] = key;
        payloads[index] = payload;
    }
    void push_back(size_t key, const T & payload) {
        keys.push_back(key);
        payloads.push_back(payload);
    }
};

typedef AbstractTable<BPayload> BuildTable;
typedef AbstractTable<PPayload> ProbeTable;

struct ResultTuple {
    size_t key;
    BPayload b_payload;
    PPayload p_payload;
};

class ResultTable {
public:
    std::vector<size_t> keys;
    std::vector<BPayload> b_payloads;
    std::vector<PPayload> p_payloads;

    ResultTable() = default;

    explicit ResultTable(size_t size){
        resize(size);
    }

    size_t size() const;
    void push_back(const ResultTuple & tuple);
    void push_back(size_t key, const BPayload & b_payload, const PPayload & p_payload);
    void resize(size_t new_size);
    void reserve(size_t new_size);
    void set(size_t index, size_t key, const BPayload & b_payload, const PPayload & p_payload);
    void set(size_t index, const ResultTuple & tuple);
    void clear();
    void memcpy(const ResultTable & other, size_t destination_offset);


};

