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

#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <arpa/inet.h>

class ClientHandler
{
public:
    ClientHandler(
        const int clientSocket,
        const sockaddr_in address,
        const int timeoutSeconds,
        const int countLimit,
        const int timeStep,
        const double ingestionRate,
        const bool varSized)
        : clientSocket(clientSocket)
        , address(address)
        , timeoutSeconds(timeoutSeconds)
        , countLimit(countLimit)
        , timeStep(timeStep)
        , ingestionRate(ingestionRate)
        , varSized(varSized)
    {
    }

    void handle()
    {
        std::cout << "New connection from " << inet_ntoa(address.sin_addr) << '\n';
        if (timeoutSeconds > 0)
        {
            std::cout << "Shutting down client in " << std::to_string(timeoutSeconds) << "s...\n";
        }

        try
        {
            //std::random_device rd;
            std::mt19937 gen(42);
            std::uniform_int_distribution valueDistrib(0, 10000);

            const auto interval
                = std::chrono::microseconds(static_cast<int64_t>(std::round(ingestionRate != 0 ? 1000000 / ingestionRate : 0)));
            std::cout << "Ingestion interval: " << static_cast<uint64_t>(interval.count()) << '\n';
            auto nextSendTime = std::chrono::high_resolution_clock::now();
            const auto startTime = nextSendTime;

            while (running && (countLimit == 0 || counter < countLimit))
            {
                if (timeoutSeconds > 0)
                {
                    if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - startTime).count()
                        >= timeoutSeconds)
                    {
                        break;
                    }
                }

                if (ingestionRate > 0)
                {
                    std::this_thread::sleep_until(nextSendTime);
                    nextSendTime += interval;
                }

                const auto value = valueDistrib(gen);
                std::string message = std::to_string(counter++) + "," + std::to_string(value) + "," + std::to_string(timestamp)
                    + (varSized ? ",str:" + std::to_string(value) : "") + '\n';

                //std::cout << "Sending message: " << message;
                send(clientSocket, message.c_str(), message.size(), 0);

                timestamp += timeStep;

                if (countLimit != 0 && counter >= countLimit)
                {
                    std::cout << "Client " << inet_ntoa(address.sin_addr) << " reached count limit of " << countLimit << '\n';
                    break;
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in client: " << e.what() << '\n';
        }
        catch (...)
        {
            std::cout << "Client " << inet_ntoa(address.sin_addr) << " disconnected\n";
        }
        cleanup();
    }

    void cleanup()
    {
        running = false;
        close(clientSocket);
        /// log("TCP server shut down.");
        std::cout << "Cleaned up connection from " << inet_ntoa(address.sin_addr) << '\n';
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    int clientSocket;
    sockaddr_in address;
    int timeoutSeconds;
    uint64_t countLimit;
    int timeStep;
    double ingestionRate;
    bool varSized;
    uint64_t counter = 0;
    int timestamp = 0;
    bool running = true;
};

class CounterServer
{
public:
    CounterServer(
        const std::string& host,
        const int port,
        const int timeoutSeconds,
        const uint64_t countLimit,
        const int timeStep,
        const double ingestionRate,
        const bool varSized)
        : host(host)
        , port(port)
        , timeoutSeconds(timeoutSeconds)
        , countLimit(countLimit)
        , timeStep(timeStep)
        , ingestionRate(ingestionRate)
        , varSized(varSized)
    {
    }

    void start()
    {
        if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        {
            std::cerr << "Failed to create server socket\n";
            cleanup(-1);
        }

        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_addr.s_addr = inet_addr(host.c_str());
        serverAddress.sin_port = htons(port);

        int success = bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress));
        if (success != 0)
        {
            std::cout << "\nShutting down server because it can not bind to server socket " << serverSocket << "...\n";
            cleanup(-1);
        }
        if (listen(serverSocket, 5) == -1)
        {
            std::cout << "\nShutting down server because it can not listen on server socket " << serverSocket << "...\n";
            cleanup(-1);
        }
        std::cout << "Server listening on " << host << ":" << port << '\n';
        std::cout << "Count limit: " << (countLimit == 0 ? "unlimited" : std::to_string(countLimit))
                  << " Ingestion rate: " << (ingestionRate == 0 ? "unlimited" : std::to_string(ingestionRate)) << '\n';

        const auto startTime = std::chrono::high_resolution_clock::now();
        try
        {
            while (running)
            {
                sockaddr_in clientAddress;
                socklen_t clientLen = sizeof(clientAddress);
                const int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientLen);
                if (clientSocket == -1)
                {
                    std::cout << "\nShutting down server because it failed to accept client on socket " << clientSocket << "...\n";
                }
                const auto elapsed
                    = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - startTime).count();
                ClientHandler client(clientSocket, clientAddress, timeoutSeconds - elapsed, countLimit, timeStep, ingestionRate, varSized);
                std::thread clientThread(&ClientHandler::handle, &client);
                clientThread.detach();
                clients.push_back(std::move(client));
                /// Clean up disconnected clients
                auto it = clients.begin();
                while (it != clients.end())
                {
                    if (!it->isRunning())
                    {
                        it = clients.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in server: " << e.what() << '\n';
            cleanup(-1);
        }
        catch (...)
        {
            std::cout << "Shutting down server...\n";
        }
        cleanup(0);
    }

    void cleanup(const int errorCode)
    {
        running = false;
        /// Clean up all client connections
        for (auto& client : clients)
        {
            client.cleanup();
        }
        /// Close server socket
        close(serverSocket);
        std::cout << "Server shutdown complete\n";
        exit(errorCode);
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    std::string host = "0.0.0.0";
    int port = 5020;
    int timeoutSeconds = 0;
    uint64_t countLimit = 0;
    int timeStep = 1;
    double ingestionRate = 0;
    bool varSized = false;
    int serverSocket;
    sockaddr_in serverAddress;
    std::vector<ClientHandler> clients;
    bool running = true;
};

int main(const int argc, char* argv[])
{
    std::string host = "0.0.0.0";
    int port = 5020;
    int timeoutSeconds = 0;
    uint64_t countLimit = 0;
    int timeStep = 1;
    double ingestionRate = 0;
    bool varSized = false;

    for (int i = 1; i < argc; ++i)
    {
        if (std::strcmp(argv[i], "--host") == 0)
        {
            host = argv[++i];
        }
        else if (std::strcmp(argv[i], "-p") == 0 || std::strcmp(argv[i], "--port") == 0)
        {
            port = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-k") == 0 || std::strcmp(argv[i], "--kill") == 0)
        {
            timeoutSeconds = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-n") == 0 || std::strcmp(argv[i], "--count-limit") == 0)
        {
            countLimit = std::stoull(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-t") == 0 || std::strcmp(argv[i], "--time-step") == 0)
        {
            timeStep = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-i") == 0 || std::strcmp(argv[i], "--ingestion-rate") == 0)
        {
            ingestionRate = std::stod(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-v") == 0 || std::strcmp(argv[i], "--var-sized") == 0)
        {
            varSized = true;
        }
    }

    try
    {
        CounterServer server(host, port, timeoutSeconds, countLimit, timeStep, ingestionRate, varSized);
        server.start();
    }
    catch (const std::exception& e)
    {
        std::cerr << "Exception in main: " << e.what() << '\n';
    }
    catch (...)
    {
        std::cerr << "Unknown exception caught in main\n";
    }
    std::cout << "finished";

    return 0;
}
