#include "../include/RedisServer.h"
#include "../include/RedisCommandHandler.h"
#include "../include/RedisDatabase.h"

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <cstring>
#include <signal.h>

// Global pointer for signal handling.
// A C signal handler can't carry user data, so we stash the running server
// here to reach it from signalHandler() when Ctrl+C is pressed.
static RedisServer* globalServer = nullptr;

// Called by the OS when SIGINT (Ctrl+C) is received. Triggers a graceful
// shutdown (which persists the DB) and then exits the process.
void signalHandler(int signum) {
    if (globalServer) {
        std::cout << "\nCaught signal " << signum << ", shutting down...\n";
        globalServer->shutdown();
    }
    exit(signum);
}

// Install signalHandler() as the handler for SIGINT (Ctrl+C).
void RedisServer::setupSignalHandler() {
    signal(SIGINT, signalHandler);
}

// Initialize members and wire up signal handling. The socket isn't created
// yet (server_socket starts at -1); that happens in run().
RedisServer::RedisServer(int port) : port(port), server_socket(-1), running(true) {
    globalServer = this;       // Let the signal handler find this instance
    setupSignalHandler();
}

// Stop accepting connections, save the database, and close the socket.
void RedisServer::shutdown() {
    running = false; // Causes the accept loop in run() to stop
    if (server_socket != -1) {
        // Before shutdown, persist the database so no data is lost.
        if (RedisDatabase::getInstance().dump("dump.my_rdb"))
            std::cout << "Database Dumped to dump.my_rdb\n";
        else
            std::cerr << "Error dumping database\n";
        close(server_socket);
    }
    std::cout << "Server Shutdown Complete!\n";
}

// Create the listening socket and serve clients until shutdown.
void RedisServer::run() {
    // Create a TCP (SOCK_STREAM) socket over IPv4 (AF_INET).
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        std::cerr << "Error Creating Server Socket\n";
        return;
    }

    // Allow immediate reuse of the port after restart (avoids "address already
    // in use" while the OS holds the old socket in TIME_WAIT).
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Warning: setsockopt SO_REUSEADDR failed\n";
    }

    // Describe the address to bind to: this port, on any local interface.
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);          // host -> network byte order
    serverAddr.sin_addr.s_addr = INADDR_ANY;    // accept connections on any IP

    // Attach the socket to the address/port.
    if (bind(server_socket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Error Binding Server Socket\n";
        close(server_socket);
        return;
    }

    // Start listening; up to 10 connections may wait in the accept backlog.
    if (listen(server_socket, 10) < 0) {
        std::cerr << "Error Listening On Server Socket\n";
        close(server_socket);
        return;
    }

    std::cout << "Redis Server Listening On Port " << port << "\n";

    std::vector<std::thread> threads;   // One worker thread per connected client
    RedisCommandHandler cmdHandler;     // Shared, stateless command parser/dispatcher

    // Accept loop: block on accept(), then hand each client off to its own thread.
    while (running) {
        int client_socket = accept(server_socket, nullptr, nullptr);
        if (client_socket < 0) {
            // accept() also fails when the socket is closed during shutdown;
            // only report it as an error if we were still meant to be running.
            if (running)
                std::cerr << "Error Accepting Client Connection\n";
            break;
        }

        // Serve this client on a dedicated thread so slow clients don't block
        // others. The thread reads requests, processes them, and writes replies
        // until the client disconnects.
        threads.emplace_back([client_socket, &cmdHandler](){
            char buffer[1024];
            while (true) {
                memset(buffer, 0, sizeof(buffer));
                // Read up to one buffer's worth of request bytes.
                int bytes = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
                if (bytes <= 0) break; // 0 = client closed; <0 = error
                std::string request(buffer, bytes);
                // Parse + execute the command and send the RESP reply back.
                std::string response = cmdHandler.processCommand(request);
                if (send(client_socket, response.c_str(), response.size(), 0) < 0) {
                    // Send failed; client connection is likely broken
                    break;
                }
            }
            close(client_socket);
        });
    }

    // Once the loop ends, wait for all client threads to finish cleanly.
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    // Final safety save on the way out (in case we exited the loop without
    // going through shutdown()).
    if (RedisDatabase::getInstance().dump("dump.my_rdb"))
        std::cout << "Database Dumped to dump.my_rdb\n";
    else
        std::cerr << "Error dumping database\n";
}
