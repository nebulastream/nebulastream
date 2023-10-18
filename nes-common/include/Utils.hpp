#pragma once

#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>

static inline void hexdump(const uint8_t* data, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        if (i % 16 == 0) {
            if (i != 0)
                std::cout << std::endl;
            std::cout << std::setw(8) << std::setfill('0') << std::hex << i << ": ";
        }
        std::cout << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(data[i]) << ' ';
    }
    std::cout << std::endl;
}
