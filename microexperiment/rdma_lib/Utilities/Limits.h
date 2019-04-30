//
// Created by Toso, Lorenzo on 2019-03-11.
//

#pragma once



struct Limits {
    size_t start_index;
    size_t end_index;

    size_t size() const{
        return end_index - start_index;
    }

    Limits() {}

    Limits(size_t start_index, size_t end_index) : start_index(start_index), end_index(end_index) {}
};
enum PartitionMode {
    PARTITIONED,
    FULL
};




