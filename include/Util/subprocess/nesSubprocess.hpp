/*
    Copyright (C) 2021 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_NESSUBPROCESS_HPP
#define NES_NESSUBPROCESS_HPP
#include <Util/subprocess/subprocess.hpp>

namespace nesSubprocess {

    class popen {

    public:
        popen(const std::string &cmd, std::vector<std::string> argv) {
        // Add error handling - cmd can be separated and ./ can be checked. Is there a full list?
        this->subprocess = subprocess::popen(cmd, argv);
        }

        popen(const std::string& cmd, std::vector<std::string> argv, std::ostream& pipe_stdout) {
        // error handling

        // was ist dieses : initialisation list called die constructoren der parent klasse
        // muss mal überprüfen, ob das einfach so läuft
        this->subprocess = subprocess::popen(cmd, argv, pipe_stdout);
        }

       // ~popen() {
        // das ist ein deconstructor der einfach das teil zerstört.
        // überprüfen, ob er das auch einfach so übernehmen würde.
        //    subprocess::~popen();
      //  }


        void forwardLogging(){
            std::thread t([this](){
                std::cout << subprocess.stdout().rdbuf();// to read out subprocess log
            });
            t.detach();
        }

        subprocess::popen subprocess;


       };


}

#endif //NES_NESSUBPROCESS_HPP
