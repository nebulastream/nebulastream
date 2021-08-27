/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORD_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORD_HPP_
#include <memory>
#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <ostream>
#include <vector>
namespace NES::QueryCompilation{

class Record{
  public:
    explicit Record(std::vector<NesValuePtr> records);
    virtual ~Record() = default;
    virtual std::shared_ptr<NesValue> read(std::string fieldName);
    virtual void write(std::string fieldName, std::shared_ptr<NesValue> value);
    friend std::ostream& operator<<(std::ostream&, const RecordPtr&);

  private:
    std::vector<NesValuePtr> records;

};

using RecordPtr = std::shared_ptr<Record>;

}

std::ostream& operator <<(std::ostream& os, const NES::QueryCompilation::RecordPtr& r);

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_RECORD_HPP_
