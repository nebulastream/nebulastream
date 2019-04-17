#include <utility>

//
// Created by Toso, Lorenzo on 2018-11-13.
//

#ifndef MAMPI_COMPUTATIONRESULT_H
#define MAMPI_COMPUTATIONRESULT_H

#include <vector>
#include "Tables.h"
#include "ComputationParameters.h"
#include "MeasuredTimes.h"
#include <json11/json11.hpp>
//#include <json.hpp>

class ComputationResult {
private:
    size_t result_tuple_count = 0;
public:
    std::string name = "";
    std::vector<ResultTable> result_tables;
    std::map<MeasuredTimes::Value, size_t> measured_times;


    ComputationResult() = default;
    ComputationResult(std::vector<ResultTable> & result_tuples, std::initializer_list<std::pair<MeasuredTimes::Value, size_t>> measured_times);
    ComputationResult(ResultTable & result_table, std::initializer_list<std::pair<MeasuredTimes::Value, size_t>> measured_times);

    void set_result_count(size_t s);
    size_t get_result_count() const;
    void print();
    size_t operator[](MeasuredTimes::Value which) const;
    void set(MeasuredTimes::Value time, size_t duration);
    Json to_json();
    void clean();
};


#endif //MAMPI_COMPUTATIONRESULT_H
