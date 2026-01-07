#include "../include/RedisCommandHandler.h"
#include "../include/RedisDatabase.h"

#include <vector>
#include <sstream>
#include <algorithm>
#include <exception>
#include <iostream> // debug

// RESP parser:
// *2\r\n$4\r\n\PING\r\n$4\r\nTEST\r\n
// *2 -> array has 2 elements
// $4 -> next string has 4 characters
// PING
// TEST
//
// Splits a raw client request into individual command tokens
// (e.g. {"SET", "name", "Alice"}). It accepts two formats:
//   1. RESP arrays (what real redis-cli sends), starting with '*'.
//   2. Plain whitespace-separated text (handy for telnet/manual testing).
std::vector<std::string> parseRespCommand(const std::string &input) {
    std::vector<std::string> tokens;
    if (input.empty()) return tokens;

    // If it doesnt strart with '*', fallback to splitting by whitespace.
    if (input[0] != '*') {
        std::istringstream iss(input);
        std::string token;
        while (iss >> token)
            tokens.push_back(token);
        return tokens;
    }

    size_t pos = 0; // current read position within the input buffer
    // Expect '*' followed by number of elements
    if (input[pos] != '*') return tokens;
    pos++; // skip '*'

    // crlf = Carriage Return (\r), Line Feed (\n)
    // RESP separates every field with "\r\n"; find the end of the count field.
    size_t crlf = input.find("\r\n", pos);
    if (crlf == std::string::npos) return tokens;

    // Read how many bulk-string elements make up this command array.
    int numElements = 0;
    try {
        numElements = std::stoi(input.substr(pos, crlf - pos));
    } catch (const std::exception&) {
        return tokens;  // malformed element count
    }
    if (numElements <= 0 || numElements > 1024)
        return tokens; // reject zero, negative, or absurdly large element counts
    pos = crlf + 2; // step past the "\r\n"

    // Read each element. Format per element: "$<len>\r\n<bytes>\r\n".
    for (int i = 0; i < numElements; i++) {
        if (pos >= input.size() || input[pos] != '$') break; // format error
        pos++; // skip '$'

        // Parse the declared byte-length of this element.
        crlf = input.find("\r\n", pos);
        if (crlf == std::string::npos) break;
        int len = 0;
        try {
            len = std::stoi(input.substr(pos, crlf - pos));
        } catch (const std::exception&) {
            break;  // malformed length
        }
        pos = crlf + 2;

        if (len < 0 || pos + static_cast<size_t>(len) > input.size()) break; // negative length or declared length runs past the buffer
        std::string token = input.substr(pos, len);
        tokens.push_back(token);
        pos += len + 2; // skip token and CRLF
    }
    return tokens;
}

// Each handle*() function below implements one Redis command. They share a
// common shape:
//   - take the parsed `tokens` (tokens[0] is the command name, the rest are
//     arguments) and a reference to the shared `db`,
//   - validate the argument count, returning a RESP error ("-Error: ...") if wrong,
//     - do the work on the database, and
//   - return a RESP-encoded reply. Common reply prefixes:
//       +  simple string (e.g. "+OK\r\n")
//       -  error
//       :  integer
//       $  bulk string ("$<len>\r\n<value>\r\n", or "$-1\r\n" for nil)
//       *  array of the above

//----------------------
// Common Commands
//----------------------

// PING -> always replies "+PONG" (used to check the connection is alive).
static std::string handlePing(const std::vector<std::string>& /*tokens*/, RedisDatabase& /*db*/) {
    return "+PONG\r\n";
}

// ECHO <message> -> replies with the same message back.
static std::string handleEcho(const std::vector<std::string>& tokens, RedisDatabase& /*db*/) {
    if (tokens.size() < 2)
        return "-Error: ECHO requires a message\r\n";
    return "+" + tokens[1] + "\r\n";
}

// FLUSHALL -> wipes the entire database (all keys, all types).
static std::string handleFlushAll(const std::vector<std::string>& /*tokens*/, RedisDatabase& db) {
    db.flushAll();
    return "+OK\r\n";
}

//----------------------
// Key/Value Operations
//----------------------
// SET <key> <value> -> store a string value, replacing any existing one.
static std::string handleSet(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: SET requires key and value\r\n";
    db.set(tokens[1], tokens[2]);
    return "+OK\r\n";
}

// GET <key> -> return the string value, or the nil bulk string ($-1) if absent.
static std::string handleGet(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: GET requires key\r\n";
    std::string value;
    if (db.get(tokens[1], value))
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    return "$-1\r\n";
}

// KEYS -> return an array of every key currently stored (any type).
static std::string handleKeys(const std::vector<std::string>& /*tokens*/, RedisDatabase& db) {
    auto allKeys = db.keys();
    std::ostringstream oss;
    oss << "*" << allKeys.size() << "\r\n";
    for (const auto& key : allKeys)
        oss << "$" << key.size() << "\r\n" << key << "\r\n";
    return oss.str();
}

// TYPE <key> -> report the value type: "string", "list", "hash", or "none".
static std::string handleType(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: TYPE requires key\r\n";
    return "+" + db.type(tokens[1]) + "\r\n";
}

// DEL/UNLINK <key> -> delete the key, replying 1 if removed or 0 otherwise.
static std::string handleDel(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: DEL requires key\r\n";
    bool res = db.del(tokens[1]);
    return ":" + std::to_string(res ? 1 : 0) + "\r\n";
}

// EXPIRE <key> <seconds> -> set a TTL on a key so it auto-deletes later.
static std::string handleExpire(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: EXPIRE requires key and time in seconds\r\n";
    try {
        int seconds = std::stoi(tokens[2]);
        if (db.expire(tokens[1], seconds))
            return "+OK\r\n";
        else
            return "-Error: Key not found\r\n";
    } catch (const std::exception&) {
        return "-Error: Invalid expiration time\r\n";
    }
}

// RENAME <oldKey> <newKey> -> move a key (and its TTL) to a new name.
static std::string handleRename(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: RENAME requires old key and new key\r\n";
    if (db.rename(tokens[1], tokens[2]))
        return "+OK\r\n";
    return "-Error: Key not found or rename failed\r\n";
}

//----------------------
// List Operations
//----------------------

// LGET <key> -> return all elements of the list as a RESP array.
static std::string handleLget(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: LGET requires a key\r\n";

    auto elems = db.lget(tokens[1]);
    std::ostringstream oss;
    oss << "*" << elems.size() << "\r\n";
    for (const auto& e : elems) {
        oss << "$" << e.size() << "\r\n"
            << e << "\r\n";
    }
    return oss.str();
}

// LLEN <key> -> reply with the number of elements in the list.
static std::string handleLlen(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: LLEN requires key\r\n";
    ssize_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

// LPUSH <key> <value...> -> prepend one or more values to the head; reply new length.
static std::string handleLpush(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: LPUSH requires key and value\r\n";
    for (size_t i = 2; i < tokens.size(); ++i) {
        db.lpush(tokens[1], tokens[i]);
    }
    ssize_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

// RPUSH <key> <value...> -> append one or more values to the tail; reply new length.
static std::string handleRpush(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: RPUSH requires key and value\r\n";
    for (size_t i = 2; i < tokens.size(); ++i) {
        db.rpush(tokens[1], tokens[i]);
    }
    ssize_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

// LPOP <key> -> remove and return the head element, or nil if the list is empty.
static std::string handleLpop(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: LPOP requires key\r\n";
    std::string val;
    if (db.lpop(tokens[1], val))
        return "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
    return "$-1\r\n";
}

// RPOP <key> -> remove and return the tail element, or nil if the list is empty.
static std::string handleRpop(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: RPOP requires key\r\n";
    std::string val;
    if (db.rpop(tokens[1], val))
        return "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
    return "$-1\r\n";
}

// LREM <key> <count> <value> -> remove matching elements; reply how many were removed.
// count > 0 removes from head, count < 0 from tail, count == 0 removes all.
static std::string handleLrem(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 4)
        return "-Error: LREM requires key, count and value\r\n";
    try {
        int count = std::stoi(tokens[2]);
        int removed = db.lrem(tokens[1], count, tokens[3]);
        return ":" + std::to_string(removed) + "\r\n";
    } catch (const std::exception&) {
        // std::stoi throws if count isn't a valid integer.
        return "-Error: Invalid count\r\n";
    }
}

// LINDEX <key> <index> -> return the element at index (negative counts from the end).
static std::string handleLindex(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: LINDEX requires key and index\r\n";
    try {
        int index = std::stoi(tokens[2]);
        std::string value;
        if (db.lindex(tokens[1], index, value)) 
            return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
        else 
            return "$-1\r\n";
    } catch (const std::exception&) {
        return "-Error: Invalid index\r\n";
    }
}

// LSET <key> <index> <value> -> overwrite the element at index (negative counts from end).
static std::string handleLset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 4)
        return "-Error: LSET requires key, index and value\r\n";
    try {
        int index = std::stoi(tokens[2]);
        if (db.lset(tokens[1], index, tokens[3]))
            return "+OK\r\n";
        else 
            return "-Error: Index out of range\r\n";
    } catch (const std::exception&) {
        return "-Error: Invalid index\r\n";
    }
}

//----------------------
// Hash Operations
//----------------------
// A hash is a key whose value is itself a map of field -> value pairs.

// HSET <key> <field> <value> -> set a single field in the hash.
// Returns 1 if the field was newly created, 0 if it was updated.
static std::string handleHset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 4)
        return "-Error: HSET requires key, field and value\r\n";
    bool isNew = db.hset(tokens[1], tokens[2], tokens[3]);
    return ":" + std::to_string(isNew ? 1 : 0) + "\r\n";
}

// HGET <key> <field> -> return one field's value, or nil if the field/key is absent.
static std::string handleHget(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: HGET requires key and field\r\n";
    std::string value;
    if (db.hget(tokens[1], tokens[2], value))
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    return "$-1\r\n";
}

// HEXISTS <key> <field> -> reply 1 if the field exists, else 0.
static std::string handleHexists(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: HEXISTS requires key and field\r\n";
    bool exists = db.hexists(tokens[1], tokens[2]);
    return ":" + std::to_string(exists ? 1 : 0) + "\r\n";
}

// HDEL <key> <field> -> remove one field; reply 1 if removed, 0 if it wasn't there.
static std::string handleHdel(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return "-Error: HDEL requires key and field\r\n";
    bool res = db.hdel(tokens[1], tokens[2]);
    return ":" + std::to_string(res ? 1 : 0) + "\r\n";
}

// HGETALL <key> -> return a flat array alternating field, value, field, value, ...
static std::string handleHgetall (const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: HGETALL requires key\r\n";
    auto hash = db.hgetall(tokens[1]);
    std::ostringstream oss;
    oss << "*" << hash.size() * 2 << "\r\n"; // *2 because each pair emits a field and a value
    for (const auto& pair: hash) {
        oss << "$" << pair.first.size() << "\r\n" << pair.first << "\r\n";
        oss << "$" << pair.second.size() << "\r\n" << pair.second << "\r\n";
    }
    return oss.str();
}

// HKEYS <key> -> return an array of all field names in the hash.
static std::string handleHkeys(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: HKEYS requires key\r\n";
    auto keys = db.hkeys(tokens[1]);
    std::ostringstream oss;
    oss << "*" << keys.size() << "\r\n";
    for (const auto& key: keys) {
        oss << "$" << key.size() << "\r\n" << key << "\r\n";
    }
    return oss.str();
}

// HVALS <key> -> return an array of all field values in the hash.
static std::string handleHvals(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: HVALS requires key\r\n";
    auto values = db.hvals(tokens[1]);
    std::ostringstream oss;
    oss << "*" << values.size() << "\r\n";
    for (const auto& val: values) {
        oss << "$" << val.size() << "\r\n" << val << "\r\n";
    }
    return oss.str();
}

// HLEN <key> -> reply with the number of fields in the hash.
static std::string handleHlen(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return "-Error: HLEN requires key\r\n";
    ssize_t len = db.hlen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

// HMSET <key> <field> <value> [<field> <value> ...] -> set many fields at once.
static std::string handleHmset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    // Need the key plus at least one field/value pair, and the trailing
    // arguments must come in pairs (so total token count must be even).
    if (tokens.size() < 4 || (tokens.size() % 2) == 1)
        return "-Error: HMSET requires key followed by field value pairs\r\n";
    std::vector<std::pair<std::string, std::string>> fieldValues;
    for (size_t i = 2; i < tokens.size(); i += 2) {
        fieldValues.emplace_back(tokens[i], tokens[i+1]); // (field, value)
    }
    db.hmset(tokens[1], fieldValues);
    return "+OK\r\n";
}

RedisCommandHandler::RedisCommandHandler() {}

// Parse one request, look up the command name, and route it to the matching
// handler. The handler's RESP reply is returned to the caller (the client thread).
std::string RedisCommandHandler::processCommand(const std::string& commandLine) {
    // Use RESP parser to split the request into command + arguments.
    auto tokens = parseRespCommand(commandLine);
    if (tokens.empty()) return "-Error: Empty command\r\n";

    // The first token is the command name; uppercase it so matching is
    // case-insensitive (e.g. "set", "Set", "SET" all work).
    std::string cmd = tokens[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    RedisDatabase& db = RedisDatabase::getInstance();

    // Dispatch table: match the command name to its handler.
    // Common Commands
    if (cmd == "PING")
        return handlePing(tokens, db);
    else if (cmd == "ECHO")
        return handleEcho(tokens, db);
    else if (cmd == "FLUSHALL")
        return handleFlushAll(tokens, db);
    // Key/Value Operations
    else if (cmd == "SET")
        return handleSet(tokens, db);
    else if (cmd == "GET")
        return handleGet(tokens, db);
    else if (cmd == "KEYS")
        return handleKeys(tokens, db);
    else if (cmd == "TYPE")
        return handleType(tokens, db);
    else if (cmd == "DEL" || cmd == "UNLINK")
        return handleDel(tokens, db);
    else if (cmd == "EXPIRE")
        return handleExpire(tokens, db);
    else if (cmd == "RENAME")
        return handleRename(tokens, db);
    // List Operations
    else if (cmd == "LGET") 
        return handleLget(tokens, db);
    else if (cmd == "LLEN") 
        return handleLlen(tokens, db);
    else if (cmd == "LPUSH")
        return handleLpush(tokens, db);
    else if (cmd == "RPUSH")
        return handleRpush(tokens, db);
    else if (cmd == "LPOP")
        return handleLpop(tokens, db);
    else if (cmd == "RPOP")
        return handleRpop(tokens, db);
    else if (cmd == "LREM")
        return handleLrem(tokens, db);
    else if (cmd == "LINDEX")
        return handleLindex(tokens, db);
    else if (cmd == "LSET")
        return handleLset(tokens, db);
    // Hash Operations
    else if (cmd == "HSET") 
        return handleHset(tokens, db);
    else if (cmd == "HGET") 
        return handleHget(tokens, db);
    else if (cmd == "HEXISTS") 
        return handleHexists(tokens, db);
    else if (cmd == "HDEL") 
        return handleHdel(tokens, db);
    else if (cmd == "HGETALL") 
        return handleHgetall(tokens, db);
    else if (cmd == "HKEYS") 
        return handleHkeys(tokens, db);
    else if (cmd == "HVALS") 
        return handleHvals(tokens, db);
    else if (cmd == "HLEN") 
        return handleHlen(tokens, db);
    else if (cmd == "HMSET")
        return handleHmset(tokens, db);
    else
        // The command name didn't match anything we support.
        return "-Error: Unknown command\r\n";
}
