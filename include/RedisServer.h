#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include <string>
#include <atomic>

// RedisServer owns the TCP listening socket and the accept loop.
// It listens for incoming client connections and spawns a thread per client
// to handle their commands. A single instance is created in main().
class RedisServer {
public:
    // Construct the server bound to the given TCP port (not yet listening).
    RedisServer(int port);

    // Create the socket, bind/listen on the port, and run the accept loop.
    // Blocks until the server is shut down. Each accepted client is served
    // on its own thread.
    void run();

    // Stop the accept loop, persist the database to disk, and close the
    // listening socket. Safe to call from a signal handler.
    void shutdown();

private:
    int port;                    // TCP port the server listens on
    int server_socket;           // File descriptor of the listening socket (-1 if not open)
    std::atomic<bool> running;   // Loop flag; set to false by shutdown() to stop accepting

    // Register the SIGINT (Ctrl+C) handler so shutdown is graceful.
    void setupSignalHandler();
};

#endif
