#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include <string>

// RedisCommandHandler turns a raw client request into a reply.
// It parses the incoming bytes (RESP protocol or plain whitespace-separated
// text), dispatches to the matching command, and produces a RESP-encoded
// response string to send back to the client.
class RedisCommandHandler {
  public:
    RedisCommandHandler();

    // Parse one client request and execute it against the shared database.
    // Returns a RESP-formatted response (e.g. "+OK\r\n", "$3\r\nfoo\r\n",
    // ":1\r\n", or "-Error: ...\r\n").
    std::string processCommand(const std::string& commandLine);
};

#endif
