#include "../include/RedisServer.h"
#include "../include/RedisDatabase.h"
#include <iostream>
#include <thread>
#include <chrono>

// Program entry point: parses the port, restores any saved data, starts a
// background save thread, and then runs the server's accept loop.
int main(int argc, char* argv[]) {
    // Use Redis' standard port 6379 unless the user passes one as argv[1].
    int port = 6379; // default
    if (argc >= 2) {
        try {
            port = std::stoi(argv[1]);
            if (port <= 0 || port > 65535) {
                std::cerr << "Error: port must be between 1 and 65535\n";
                return 1;
            }
        } catch (const std::exception&) {
            std::cerr << "Error: invalid port number\n";
            return 1;
        }
    }

    // Try to restore the previous session's data from the dump file so the
    // database survives restarts. A missing file just means a fresh start.
    if (RedisDatabase::getInstance().load("dump.my_rdb"))
        std::cout << "Database Loaded From dump.my_rdb\n";
    else
        std::cout << "No dump found or load failed; starting with an empty database.\n";

    RedisServer server(port);

    // Backgroung persistance: dump the database every 300 seconds. (5 * 60 seconds save database)
    // Runs on its own detached thread so periodic saving doesn't block the
    // server. (Note: this loop never exits on its own; the final save still
    // happens in RedisServer::shutdown()/run().)
    std::thread persistanceThread([](){
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(300));
            if (!RedisDatabase::getInstance().dump("dump.my_rdb"))
                std::cerr << "Error Dumping Database\n";
            else
                std::cout << "Database Dumped to dump.my_rdb\n";
        }
    });
    persistanceThread.detach(); // Let it run independently of main()

    // Enter the accept loop. Blocks here until the server is shut down
    // (e.g. via Ctrl+C, handled in RedisServer).
    server.run();
    return 0;
}
