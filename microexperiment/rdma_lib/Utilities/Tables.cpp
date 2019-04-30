//
// Created by Toso, Lorenzo on 2019-01-25.
//

#include <cstring>
#include "Tables.h"


/*template <class T>
size_t AbstractTable<T>::size()  const{
    return keys.size();
}* /

void BuildTable::resize(size_t new_size) {
    keys.resize(new_size);
    payloads.resize(new_size);
}

void BuildTable::push_back(size_t key, const BPayload &b_payload) {
    keys.push_back(key);
    payloads.push_back(b_payload);
}

void BuildTable::reserve(size_t new_size) {
    keys.reserve(new_size);
    payloads.reserve(new_size);
}

void BuildTable::set(size_t index, size_t key, const BPayload &b_payload) {
    keys[index] = key;
    payloads[index] = b_payload;
}

void BuildTable::clear() {
    keys.clear();
    payloads.clear();
}*/



size_t ResultTable::size()  const{
    return keys.size();
}

void ResultTable::push_back(const ResultTuple &tuple) {
    keys.push_back(tuple.key);
    b_payloads.push_back(tuple.b_payload);
    p_payloads.push_back(tuple.p_payload);
}

void ResultTable::push_back(size_t key, const BPayload &b_payload, const PPayload &p_payload) {
    keys.push_back(key);
    b_payloads.push_back(b_payload);
    p_payloads.push_back(p_payload);
}

void ResultTable::resize(size_t new_size) {
    keys.resize(new_size);
    b_payloads.resize(new_size);
    p_payloads.resize(new_size);
}

void ResultTable::reserve(size_t new_size) {
    keys.reserve(new_size);
    b_payloads.reserve(new_size);
    p_payloads.reserve(new_size);
}

void ResultTable::set(size_t index, size_t key, const BPayload &b_payload, const PPayload &p_payload) {
    keys[index] = key;
    b_payloads[index] = b_payload;
    p_payloads[index] = p_payload;

}

void ResultTable::set(size_t index, const ResultTuple &tuple) {
    set(index, tuple.key, tuple.b_payload, tuple.p_payload);
}

void ResultTable::clear() {
    keys.clear();
    b_payloads.clear();
    p_payloads.clear();
}

void ResultTable::memcpy(const ResultTable &other, size_t destination_offset) {
    std::memcpy(keys.data() + destination_offset, other.keys.data(), other.size() * sizeof(size_t));
    std::memcpy(p_payloads.data() + destination_offset, other.p_payloads.data(), other.size() * sizeof(PPayload));
    std::memcpy(b_payloads.data() + destination_offset, other.b_payloads.data(), other.size() * sizeof(BPayload));
}
