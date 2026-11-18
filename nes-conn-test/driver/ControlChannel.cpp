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

#include <ControlChannel.hpp>

#include <array>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

#include <boost/asio/buffer.hpp>
#include <boost/asio/local/stream_protocol.hpp>
/// The include cleaner maps read_until and write to their impl/ headers, which do not compile without these public ones.
#include <boost/asio/read_until.hpp> /// NOLINT(misc-include-cleaner)
#include <boost/asio/write.hpp> /// NOLINT(misc-include-cleaner)
#include <boost/system/detail/error_code.hpp>

namespace NES::ConnTest
{

ControlChannel::ControlChannel(const std::string_view path)
{
    socket.connect(boost::asio::local::stream_protocol::endpoint{path});
}

std::optional<std::string> ControlChannel::readLine()
{
    boost::system::error_code errorCode;
    /// Returns at once when the buffer already holds a line, and reports EOF only when no complete line is left.
    /// The runner sends only whole lines, so unterminated trailing bytes are dropped.
    /// NOLINTNEXTLINE(misc-include-cleaner): provided by <boost/asio/read_until.hpp>, see the includes.
    const std::size_t lineLength = boost::asio::read_until(socket, boost::asio::dynamic_buffer(buffer), '\n', errorCode);
    if (errorCode)
    {
        return std::nullopt;
    }
    std::string line = buffer.substr(0, lineLength - 1);
    buffer.erase(0, lineLength);
    if (!line.empty() && line.back() == '\r')
    {
        line.pop_back();
    }
    return line;
}

bool ControlChannel::writeLine(std::string_view line)
{
    const std::array buffers{boost::asio::buffer(line), boost::asio::buffer(std::string_view{"\n"})};
    boost::system::error_code errorCode;
    /// NOLINTNEXTLINE(misc-include-cleaner): provided by <boost/asio/write.hpp>, see the includes.
    boost::asio::write(socket, buffers, errorCode);
    return !errorCode;
}

}
