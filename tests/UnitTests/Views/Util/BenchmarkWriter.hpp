//
// Created by Adrian Michalke on 03.05.22.
//

#ifndef NES_BENCHMARKWRITER_HPP
#define NES_BENCHMARKWRITER_HPP

std::string currentDate(time_t now) {
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d", &tstruct);
    return buf;
}
std::string currentTime(time_t now) {
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%H-%M", &tstruct);
    return buf;
}

static void writeResult(std::string line) {
    time_t now = time(0);
    std::ofstream o;
    std::string a = std::string("../../benchmark_result/") + currentDate(now) + std::string(".csv");
    o.open(a.c_str(), std::ios_base::app);
    o << currentTime(now) << ", "<< line << "\n";
    o.close();
}

#endif //NES_BENCHMARKWRITER_HPP
