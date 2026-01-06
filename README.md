# Mini Redis Server in C++

A lightweight Redis-compatible in-memory data store written from scratch in C++17. Implements the Redis Serialization Protocol (RESP), supports strings, lists, and hashes, handles multiple concurrent clients via threads, and persists data to disk automatically.

---

## Features

- Full **RESP protocol** parsing (both inline and array formats)
- **String, List, and Hash** data types
- **Multi-client concurrency** — each connection runs in its own `std::thread`
- **TTL / expiration** via lazy eviction
- **Disk persistence** — auto-dump every 5 minutes and on graceful shutdown
- Drop-in compatible with `redis-cli`

---

## Repository Structure

```
├── include/
│   ├── RedisCommandHandler.h
│   ├── RedisDatabase.h
│   └── RedisServer.h
├── src/
│   ├── RedisCommandHandler.cpp
│   ├── RedisDatabase.cpp
│   ├── RedisServer.cpp
│   └── main.cpp
├── Concepts,UseCases&Tests.md
├── Makefile
└── test_all.sh
```

---

## Build

Requires a C++17 compiler and pthreads.

```bash
make
```

Or manually:

```bash
g++ -std=c++17 -pthread -Iinclude src/*.cpp -o my_redis_server
```

---

## Usage

```bash
./my_redis_server          # listens on port 6379
./my_redis_server 6380     # custom port
```

On startup the server tries to load `dump.my_rdb`. If found, data from the previous session is restored. Press `Ctrl+C` to trigger a final dump and shut down cleanly.

Connect with `redis-cli` or any RESP client:

```bash
redis-cli -p 6379

127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET name "arghadeep"
OK
127.0.0.1:6379> GET name
"arghadeep"
```

---

## Supported Commands

### General
| Command | Syntax | Description |
|---------|--------|-------------|
| PING | `PING` | Returns PONG |
| ECHO | `ECHO <msg>` | Returns the message |
| FLUSHALL | `FLUSHALL` | Clears all data |

### Strings
| Command | Syntax | Description |
|---------|--------|-------------|
| SET | `SET <key> <value>` | Store a string |
| GET | `GET <key>` | Retrieve a string |
| KEYS | `KEYS *` | List all keys |
| TYPE | `TYPE <key>` | Returns type: string/list/hash/none |
| DEL / UNLINK | `DEL <key>` | Delete a key |
| EXPIRE | `EXPIRE <key> <seconds>` | Set a TTL |
| RENAME | `RENAME <old> <new>` | Rename a key |

### Lists
| Command | Syntax | Description |
|---------|--------|-------------|
| LPUSH / RPUSH | `LPUSH <key> <v1> [v2 ...]` | Push one or more elements |
| LPOP / RPOP | `LPOP <key>` | Pop from left or right |
| LGET | `LGET <key>` | Return all elements |
| LLEN | `LLEN <key>` | Length of list |
| LINDEX | `LINDEX <key> <index>` | Get element by index |
| LSET | `LSET <key> <index> <value>` | Set element by index |
| LREM | `LREM <key> <count> <value>` | Remove occurrences of value |

### Hashes
| Command | Syntax | Description |
|---------|--------|-------------|
| HSET | `HSET <key> <field> <value>` | Set a field |
| HGET | `HGET <key> <field>` | Get a field |
| HMSET | `HMSET <key> <f1> <v1> [f2 v2 ...]` | Set multiple fields |
| HGETALL | `HGETALL <key>` | All field/value pairs |
| HKEYS | `HKEYS <key>` | All fields |
| HVALS | `HVALS <key>` | All values |
| HLEN | `HLEN <key>` | Number of fields |
| HEXISTS | `HEXISTS <key> <field>` | Check field existence |
| HDEL | `HDEL <key> <field>` | Delete a field |

---

## Architecture

- **Singleton database** — `RedisDatabase::getInstance()` ensures one shared in-memory store across all threads.
- **Three separate stores** — `kv_store` (strings), `list_store` (lists), `hash_store` (hashes), each an `unordered_map`.
- **Mutex-guarded access** — a single `std::mutex` serializes all reads and writes.
- **Lazy TTL eviction** — `purgeExpired()` is called on each access; no background sweeper needed.
- **Custom RESP parser** — handles both inline commands and the `*N\r\n$N\r\n...` array format.
- **Text-based RDB** — `dump.my_rdb` is a simple text file; no binary encoding complexity.

---

## Testing

Run the full test suite against a live server instance:

```bash
./test_all.sh
```

Detailed command use cases and expected outputs are documented in [Concepts,UseCases&Tests.md](Concepts,UseCases&Tests.md).
