//
// Created by Toso, Lorenzo on 2018-12-12.
//

//#ifndef KEY_ASSIGNMENT_H
//#define KEY_ASSIGNMENT_H

#pragma once

#include <cstddef>
#include <functional>


class KeyAssignment
{
public:
    int rank = 0;
    size_t min_key = 0;
    size_t max_key = 0;

    KeyAssignment() = default;

    KeyAssignment(int rank, size_t min_key, size_t max_key)
    : rank(rank)
    , min_key(min_key)
    , max_key(max_key)
    {     }

    std::function<bool(size_t)> get_matcher() const {
        return [this](size_t key){ return key >= min_key && key < max_key; };
    }
};

//#endif

