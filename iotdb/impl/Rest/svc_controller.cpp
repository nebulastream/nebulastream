//
//  Created by Ivan Mejia on 12/24/16.
//
// MIT License
//
// Copyright (c) 2016 ivmeroLabs.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include <Rest/std_service.hpp>
#include <Rest/svc_controller.hpp>

using namespace web;
using namespace http;

void ServiceController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&ServiceController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&ServiceController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&ServiceController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&ServiceController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&ServiceController::handlePatch, this, std::placeholders::_1));
}

void ServiceController::handleGet(http_request message) {
    auto path = requestPath(message);
    if (!path.empty()) {
        if (path[0] == "service" && path[1] == "test") {
            auto response = json::value::object();
            response["version"] = json::value::string("0.1.1");
            response["status"] = json::value::string("ready!");
            message.reply(status_codes::OK, response);
        }
    }
    else {
        message.reply(status_codes::NotFound);
    }
}

void ServiceController::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH));
}

void ServiceController::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT));
}

void ServiceController::handlePost(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::POST));
}

void ServiceController::handleDelete(http_request message) {    
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL));
}

void ServiceController::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD));
}

void ServiceController::handleOptions(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::OPTIONS));
}

void ServiceController::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE));
}

void ServiceController::handleConnect(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT));
}

void ServiceController::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE));
}

json::value ServiceController::responseNotImpl(const http::method & method) {
    auto response = json::value::object();
    response["serviceName"] = json::value::string("C++ Mircroservice Sample");
    response["http_method"] = json::value::string(method);
    return response ;
}