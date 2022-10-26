/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#ifndef NES_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_HPP_
#define NES_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_HPP_
#include "Nautilus/Interface/DataTypes/Value.hpp"
#include <Nautilus/Interface/DataTypes/Any.hpp>
namespace NES::Nautilus {

class TimeStamp : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<TimeStamp>();

    TimeStamp(Value<> milliseconds);
    Nautilus::IR::Types::StampPtr getType() const override;
    AnyPtr add(const TimeStamp& other) const;

    Value<> getMilliSeconds();
    Value<> getSeconds();
    Value<> getMinutes();
    Value<> getHours();
    Value<> getDays();
    Value<> getMonths();
    Value<> getYears();

    AnyPtr copy() override;
    Value<> getValue();
    std::string toString() override;
  private:
    Value<> milliseconds;
};

}// namespace Nautilus

#endif//NES_NES_RUNTIME_SRC_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_H_
