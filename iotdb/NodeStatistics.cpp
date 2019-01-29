#include <stdio.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <array>
#include <vector>
#include <chrono>
#include <thread>
#include <exception>

/* 
 * This class implements the gathering of node specific informations
 * The commands were tested on an Ubuntu 16.04
 *
 * Author: adrian
 */


/*
 *  Execute a commandline and return the result
 */
std::string exec(const char* cmd) {
    std::array<char, 128> buffer;
    std::string result;

    auto pipe = popen(cmd, "r");

    if (!pipe) throw std::runtime_error("popen() failed!");

    while (!feof(pipe)) {
        if (fgets(buffer.data(), 128, pipe) != nullptr)
            result += buffer.data();
    }

    auto rc = pclose(pipe);

    if (rc == EXIT_SUCCESS) { // == 0

    } else if (rc == EXIT_FAILURE) {  // EXIT_FAILURE is not used by all programs, maybe needs some adaptation.

    }
    return result;
}


/*
 * Provide the number of CPUs
 *
 * It uses the following commandline:
 * lscpu | grep 'CPU(s):' |  awk '{print $2}' | head -1
 * 
 */
int getCPUCount(){
    std::string resultString = exec("lscpu | grep 'CPU(s):' |  awk '{print $2}' | head -1");
    int count = 0;
    try {
        count = std::stoi(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getCPUCount failed!" << std::endl;
    }
    return count;
}   

/*
 * Provide CPU Additional Information
 *
 * It uses the following commandline:
 * lscpu | grep 'Flags' | awk '{ for(i=2; i<NF; i++) printf "%s",$i OFS; if(NF) printf "%s",$NF; printf ORS}' | head -1
 * 
 * The command 
 * awk '{ for(i=2; i<NF; i++) printf "%s",$i OFS; if(NF) printf "%s",$NF; printf ORS}' 
 * prints all columns without the first col
 *
 */
std::vector<std::string>  getCPUAdditionalInformation(){
    std::string resultString = exec("lscpu | grep 'Flags' | awk '{ for(i=2; i<NF; i++) printf \"%s\",$i OFS; if(NF) printf \"%si\",$NF; printf ORS}' | head -1");
    try {
        size_t pos = 0;
        std::vector<std::string> results;
        std::string token;
        std::string delimiter = "\n"; 
        while ((pos = resultString.find(delimiter)) != std::string::npos) {
            token = resultString.substr(0, pos);
            results.push_back(token);
            resultString.erase(0, pos + delimiter.length());
        }
        return results;
    } catch (std::exception& e) {
        std::cout << "Parsing in getCPUAdditionalInformation failed!" << std::endl;
        return std::vector<std::string>();
    }
}

/*
 * Provide current CPU usage in percent
 *
 * It uses the following commandline:
 * top -b -d1 -n1 | grep -i "Cpu(s)"
 *
 */
float getCPUUsage(){
    std::string resultString = exec("top -b -d1 -n1 | grep -i 'Cpu(s)'");
    float count = 0;
    try {
        count = std::stof(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getCPUUsage failed!" << std::endl;
        count = -1;
    }
    return count;
}

/*
 * Provide CPU usage in percent for a time window in sec
 * Measure the load in 10 msec steps
 */
float getCPUUsageforWindow(unsigned int window_in_sec){
    float count = 0;
    int iter = 0;
    auto start = std::chrono::high_resolution_clock::now();
    while (true){
        iter++;

        float i = getCPUUsage();
        if (i == -1){
            std::cout << "Error in getCPUUsage! getCPUUsageforWindow failed!" << std::endl;
            return -1;
        }
        count += i;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto finish = std::chrono::high_resolution_clock::now();
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(finish-start);
        if (microseconds > std::chrono::seconds(window_in_sec))
            break;
    }
    count /= iter;
    return count;
}

/*
 * Provide main memory capacity in kB 
 *
 * It uses the following commandline:
 *  top -b -d1 -n1 | grep -i "KiB Mem" | awk '{print $4}'
 *
 */
int getMainMemoryCapacity(){
    std::string resultString = exec("top -b -d1 -n1 | grep -i 'KiB Mem' | awk '{print $4}'");
    int count = 0;
    try {
        count = std::stoi(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getMainMemoryCapacity failed!" << std::endl;
    }
    return count;
}


/*
 * Provide Disk capacity in Kb
 *
 * It uses the following commandline:
 * df -k | awk '{n += $2}; END{print n}'
 *
 */
int getDiskCapacity(){
    std::string resultString = exec("df -k | awk '{n += $2}; END{print n}'");
    int count = 0;
    try {
        count = std::stoi(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getDiskCapacity failed!" << std::endl;
    }
    return count;
}

/*
 * Provide avaliable disk capacity in Kb
 *
 * It uses the following commandline:
 * df -k | awk '{n += $4}; END{print n}'
 *
 */
int getAvailableDiskCapacity(){
    std::string resultString = exec("df -k | awk '{n += $4}; END{print n}'");
    int count = 0;
    try {
        count = std::stoi(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getAvailableDiskCapacity failed!" << std::endl;
    }
    return count;
}

/*
 *  Provide list of network interfaces
 *
 *  It uses the following commandline:
 *  ifconfig -s | awk '{ print $1; }' | sed '1d'
 *
 */
std::vector<std::string> getNetworkInterfaces(){
    std::string resultString = exec("ifconfig -s | awk '{ print $1; }' | sed '1d'");
    
    try {
        size_t pos = 0;
        std::vector<std::string> results;
        std::string token;
        std::string delimiter = "\n"; 
        while ((pos = resultString.find(delimiter)) != std::string::npos) {
            token = resultString.substr(0, pos);
            results.push_back(token);
            resultString.erase(0, pos + delimiter.length());
        }
        return results;
    } catch (std::exception& e) {
        std::cout << "Parsing in getNetworkInterfaces failed!" << std::endl;
        return std::vector<std::string>();
    }
}

/*
 *  Provide Speed of a network interface
 *
 *  It uses the following commandline:
 *  ethtool interface_name | grep "speed"
 *
 */
int getNetworkInterfaceSpeed(std::string interface_name){
    std::string resultString = exec(std::string(std::string("ethtool ") + interface_name + std::string(" | grep 'speed'")).c_str());
    int count = 0; 
    try {
        count = std::stoi(resultString);
    } catch (std::exception& e) {
        std::cout << "Parsing in getNetworkInterfaceSpeed failed!" << std::endl;
    }
    return count;
}

/*
 *  Provide the current network load for a given interface
 *
 *  It uses the following commandline:
 *  ? nload ?
 *
 */
int getNetworkInterfaceLoad(std::string interface_name){
    // TODO
    int count = -1; 
    return count;
}

/*
 *  Provide the current network load for a given interface and time window in sec
 *
 *  It uses the following commandline:
 *  ? nload ?
 *
 */
float getNetworkInterfaceLoadforWindow(std::string interface_name, int window_in_sec){
    float count = 0;
    int iter = 0;
    auto start = std::chrono::high_resolution_clock::now();
    while (true){
        iter++;   
        
        float i = getNetworkInterfaceLoad(interface_name);
        if (i == -1){
            std::cout << "Error in getCPUUsage! getCPUUsageforWindow failed!" << std::endl;
            return -1;
        }
        count += i;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto finish = std::chrono::high_resolution_clock::now();
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(finish-start);
        if (microseconds > std::chrono::seconds(window_in_sec))
            break;
    }
    count /= iter;
    return count;
}

/* 
 * Main func for testing
 *
 */
int main (){
    std::cout << "getCPUCount(): " <<  getCPUCount() << std::endl;
    std::cout << "getCPUUsage(): " <<  getCPUUsage() << std::endl;
   //std::vector<std::string>  getCPUAdditionalInformation(){
    std::cout << "getCPUUsageforWindow(10): " << getCPUUsageforWindow(10) << std::endl;
    std::cout << "getDiskCapacity(): " << getDiskCapacity() << std::endl;
    std::cout << "getAvailableDiskCapacity(): " << getAvailableDiskCapacity() << std::endl;
    //std::vector<std::string> getNetworkInterfaces(){
    std::cout << "getNetworkInterfaceSpeed('wlan0'): " << getNetworkInterfaceSpeed("wlan0") << std::endl;
    std::cout << "getNetworkInterfaceLoad('wlan0'):  " << getNetworkInterfaceLoad("wlan0") << std::endl;
    std::cout << "getNetworkInterfaceLoadforWindow('wlan0', 10): " <<  getNetworkInterfaceLoadforWindow("wlan0", 10) << std::endl;
}

