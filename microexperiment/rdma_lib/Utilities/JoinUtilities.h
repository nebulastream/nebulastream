//
// Created by Toso, Lorenzo on 2019-03-05.
//

#pragma once
#include <cstddef>


class JoinUtilities {
public:
    struct Limits {
        size_t start_index;
        size_t end_index;
        size_t size() const{
            return end_index - start_index;
        }
    };
    enum PartitionMode {
        PARTITIONED,
        FULL
    };
};



namespace VectorUtils {
    template <typename T>
    void deallocate(T & vec){
        vec.resize(0);
        vec.shrink_to_fit();
    }
};
