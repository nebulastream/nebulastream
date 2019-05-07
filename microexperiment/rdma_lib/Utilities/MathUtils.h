//
// Created by Toso, Lorenzo on 2019-01-23.
//

#ifndef MAMPI_MATHUTILS_H
#define MAMPI_MATHUTILS_H

#include <algorithm>
#include <cmath>
namespace Math {
    template <typename It> static double median(It begin, It end){
        using T = typename std::iterator_traits<It>::value_type;
        std::vector<T> data(begin, end);
        std::nth_element(data.begin(), data.begin() + data.size() / 2, data.end());
        return data[data.size() / 2];
    }

    template <typename It> static double std(It begin, It end) {
        using T = typename std::iterator_traits<It>::value_type;
        std::vector<T> data(begin, end);

        double sum = std::accumulate(begin, end, 0.0);
        double mean = sum / data.size();

        double sq_sum = std::inner_product(begin, end, begin, 0.0);
        double stdev = std::sqrt(sq_sum / data.size() - mean * mean);
        return stdev;
    }
}



#endif //MAMPI_MATHUTILS_H
