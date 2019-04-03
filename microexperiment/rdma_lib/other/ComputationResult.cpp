//
// Created by Toso, Lorenzo on 2019-01-23.
//
#include "ComputationResult.h"
#include <algorithm>

ComputationResult::ComputationResult(std::vector<ResultTable> & result_tuples, std::initializer_list<std::pair<MeasuredTimes::Value, size_t>> measured_times)
:result_tables(std::move(result_tuples))
{
    for(auto & time : measured_times){
        this->measured_times[time.first] = time.second;
    }
}
ComputationResult::ComputationResult(ResultTable &result_tuples, std::initializer_list<std::pair<MeasuredTimes::Value, size_t>> measured_times) {
    result_tables.emplace_back(std::move(result_tuples));

    for(auto & time : measured_times){
        this->measured_times[time.first] = time.second;
    }
}

void ComputationResult::print(){
    printf("---%s---\n", name.c_str());
    printf("Computation Time: %luns (%lums)\n", measured_times[MeasuredTimes::TOTAL], measured_times[MeasuredTimes::TOTAL]/1000000);
    printf("Result tuple count: %lu\n", get_result_count());
    printf("-------\n");
}
size_t ComputationResult::operator[](MeasuredTimes::Value which) const {
    if(measured_times.find(which) != measured_times.end()) {
        size_t time = measured_times.at(which);
        return time;
    }
    else
        return 0;
}
void ComputationResult::set(MeasuredTimes::Value time, size_t duration){
    measured_times[time] = duration;
}

Json ComputationResult::to_json() {
    Json::object j;
    j["Name"] = name;
    j["TupleCount"] = (double)(get_result_count());
    for(EACH_MEASURED_TIME){
        j[MeasuredTimes::to_string(time)] = (double)measured_times[time];
    }
    return j;
}

void ComputationResult::clean() {
    if (result_tuple_count == 0)
        result_tuple_count = get_result_count();
    result_tables.resize(0);
}

size_t ComputationResult::get_result_count() const {
    if (result_tables.empty())
        return result_tuple_count;
    return std::accumulate(result_tables.begin(), result_tables.end(), (size_t)0, [](size_t s, auto &r){ return s + r.size(); });
}

void ComputationResult::set_result_count(size_t s) {
    result_tuple_count = s;

}

