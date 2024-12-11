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
        int clientSocket,
        sockaddr_in address,
        double interval,
        int countLimit,
        int timeStep,
        double unorderedness,
        int minDelay,
        int maxDelay,
        std::string& logFileName)
        : clientSocket(clientSocket)
        , address(address)
        , interval(interval)
        , countLimit(countLimit)
        , timeStep(timeStep)
        , unorderedness(unorderedness)
        , minDelay(minDelay)
        , maxDelay(maxDelay)
        , logFileName(logFileName)
    {
    }

    void handle()
    {
        // logFile.open(logFileName, std::ios_base::app);
        // if (!logFile.is_open())
        // {
        //     std::cerr << "Failed to open log file: " << logFileName << std::endl;
        // }
        // log("TCP server started");
        std::cout << "New connection from " << inet_ntoa(address.sin_addr) << std::endl;
        try
        {
            // define random distributions for unorderedness and ts delay
            std::random_device rd;
            std::mt19937 gen(rd());

            std::uniform_real_distribution<> unorderednessDistrib(0, 1);
            std::uniform_int_distribution<> valueDistrib(minDelay, 10000);
            std::uniform_int_distribution<> delayDistrib(minDelay, maxDelay);

            while (running && (countLimit == -1 || counter < countLimit))
            {
                auto startTime = std::chrono::high_resolution_clock::now();
                value = valueDistrib(gen);

                int currentTs = timestamp;
                // add random delay for some tuples with the percentage of unorderedness
                if (unorderednessDistrib(gen) < unorderedness)
                {
                    int delay = delayDistrib(gen);
                    if (currentTs - delay >= 0 && std::rand() % 2)
                    {
                        // add negative delay randomly, if possible
                        currentTs -= delay;
                    }
                    else
                    {
                        // or add positive delay
                        currentTs += delay;
                    }
                }

                std::string message = std::to_string(counter) + "," + std::to_string(value) + "," + std::to_string(payload) + ","
                    + std::to_string(currentTs) + "\n";
                send(clientSocket, message.c_str(), message.size(), 0);

                counter++;
                timestamp += timeStep;

                if (countLimit != -1 && counter >= countLimit)
                {
                    std::cout << "Client " << inet_ntoa(address.sin_addr) << " reached count limit of " << countLimit << std::endl;
                    break;
                }

                auto endTime = std::chrono::high_resolution_clock::now();
                auto executionTime = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime).count();
                if (executionTime > interval)
                {
                    // log("TCP server could not match given interval.");
                    std::cout << "TCP server could not match given interval." << std::endl;
                }
                else
                {
                    // std::this_thread::sleep_for(std::chrono::duration<double>(interval - executionTime));
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cerr << "Exception in client: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cout << "Client " << inet_ntoa(address.sin_addr) << " disconnected" << std::endl;
        }
        cleanup();
    }

    void cleanup()
    {
        running = false;
        close(clientSocket);
        // log("TCP server shut down.");
        std::cout << "Cleaned up connection from " << inet_ntoa(address.sin_addr) << std::endl;
    }

    void log(const std::string& message)
    {
        if (logFile.is_open())
        {
            logFile << message << std::endl;
            logFile.flush();
        }
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    int clientSocket;
    sockaddr_in address;
    double interval;
    int countLimit;
    int timeStep;
    double unorderedness;
    int minDelay;
    int maxDelay;
    int counter = 0;
    int value = 0;
    int payload = 0;
    int timestamp = 0;
    bool running = true;
    std::ofstream logFile;
    std::string logFileName;
};

class CounterServer
{
public:
    CounterServer(
        const std::string& host, int port, double interval, int countLimit, int timeStep, double unorderedness, int minDelay, int maxDelay)
        : host(host)
        , port(port)
        , interval(interval)
        , countLimit(countLimit)
        , timeStep(timeStep)
        , unorderedness(unorderedness)
        , minDelay(minDelay)
        , maxDelay(maxDelay)
    {
        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_addr.s_addr = inet_addr(host.c_str());
        serverAddress.sin_port = htons(port);
    }

    void start()
    {
        int success = bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress));
        if (success != 0)
        {
            std::cout << "\nShutting down server..." << std::endl;
            cleanup();
        }
        listen(serverSocket, 5);
        std::cout << "Server listening on " << host << ":" << port << std::endl;
        std::cout << "Counter interval: " << interval << " seconds" << std::endl;
        std::cout << "Count limit: " << (countLimit == -1 ? "unlimited" : std::to_string(countLimit)) << std::endl;
        std::cout << "timeStep: " << timeStep << ", unorderedness: " << unorderedness << ", minDelay: " << minDelay
                  << ", maxDelay: " << maxDelay << std::endl;

        try
        {
            while (running)
            {
                sockaddr_in clientAddress;
                socklen_t clientLen = sizeof(clientAddress);
                int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientLen);
                std::string logFileName = "tcp_server_" + std::to_string(port) + ".log";
                ClientHandler client(
                    clientSocket, clientAddress, interval, countLimit, timeStep, unorderedness, minDelay, maxDelay, logFileName);
                std::thread clientThread(&ClientHandler::handle, &client);
                clientThread.detach();
                clients.push_back(std::move(client));
                // Clean up disconnected clients
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
            std::cerr << "Exception in server: " << e.what() << std::endl;
        }
        catch (...)
        {
            std::cout << "Shutting down server..." << std::endl;
        }
        cleanup();
    }

    void cleanup()
    {
        running = false;
        // Clean up all client connections
        for (auto& client : clients)
        {
            client.cleanup();
        }
        // Close server socket
        close(serverSocket);
        std::cout << "Server shutdown complete" << std::endl;
    }

    [[nodiscard]] bool isRunning() const { return running; }

private:
    std::string host = "0.0.0.0";
    int port = 5020;
    double interval = 0.0001;
    int countLimit = -1;
    int timeStep = 1;
    double unorderedness = 0.0;
    int minDelay = 0;
    int maxDelay = 0;
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serverAddress;
    std::vector<ClientHandler> clients;
    bool running = true;
};

int main(int argc, char* argv[])
{
    std::string host = "0.0.0.0";
    int port = 5020;
    double interval = 0.0001;
    int countLimit = -1;
    int timeStep = 1;
    double unorderedness = 0.0;
    int minDelay = 0;
    int maxDelay = 0;

    for (int i = 1; i < argc; i++)
    {
        if (std::strcmp(argv[i], "--host") == 0)
        {
            host = argv[++i];
        }
        else if (std::strcmp(argv[i], "-p") == 0 || std::strcmp(argv[i], "--port") == 0)
        {
            port = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-i") == 0 || std::strcmp(argv[i], "--interval") == 0)
        {
            interval = std::stod(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-n") == 0 || std::strcmp(argv[i], "--count-limit") == 0)
        {
            countLimit = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-t") == 0 || std::strcmp(argv[i], "--time-step") == 0)
        {
            timeStep = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-u") == 0 || std::strcmp(argv[i], "--unorderedness") == 0)
        {
            unorderedness = std::stod(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-d") == 0 || std::strcmp(argv[i], "--min-delay") == 0)
        {
            minDelay = std::stoi(argv[++i]);
        }
        else if (std::strcmp(argv[i], "-m") == 0 || std::strcmp(argv[i], "--max-delay") == 0)
        {
            maxDelay = std::stoi(argv[++i]);
        }
    }

    try {
        CounterServer server(host, port, interval, countLimit, timeStep, unorderedness, minDelay, maxDelay);
        server.start();
    } catch (const std::exception& e){
        std::cerr << "Exception in main: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception caught in main" << std::endl;
    }
    std::cout << "finished";

    return 0;
}
