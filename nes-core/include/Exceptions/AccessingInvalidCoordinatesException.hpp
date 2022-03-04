/*
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
#ifndef NES_ACCESSINGINVALIDCOORDINATESEXCEPTION_H
#define NES_ACCESSINGINVALIDCOORDINATESEXCEPTION_H

#include <exception>
namespace NES {
/**
 * @brief this exceaption is thrown when the latitude or longitude of a GegraphicalLocation object representing an invalid location
 * is accessed
 */
class AccessingInvalidCoordinatesException : public std::exception{
    const char* what () const noexcept;
};
}

#endif//NES_ACCESSINGINVALIDCOORDINATESEXCEPTION_H
