#ifndef REDIS_DATABASE_H
#define REDIS_DATABASE_H

#include <string>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <chrono>

// RedisDatabase is the in-memory data store backing the server.
//
// It is implemented as a thread-safe singleton: every client thread shares
// the one instance, and a single mutex (db_mutex) serializes all access so
// concurrent commands don't corrupt the data.
//
// Three independent stores hold the three supported value types:
//   - kv_store    : simple string key -> value     (GET/SET)
//   - list_store  : key -> ordered list of strings (LPUSH/RPUSH/...)
//   - hash_store  : key -> field -> value map       (HSET/HGET/...)
// A key normally lives in exactly one store depending on its type.
class RedisDatabase {
  public:
    // Return the single shared instance (created on first call).
    static RedisDatabase& getInstance();

    // Common Commands
    // Wipe every store, removing all keys of all types.
    bool flushAll();

    // Key/Value Operations
    void set(const std::string& key, const std::string& value);        // Store/overwrite a string value
    bool get(const std::string& key, std::string& value);              // Fetch a string value; false if absent
    std::vector<std::string> keys();                                   // List every key across all stores
    std::string type(const std::string& key);                          // "string" / "list" / "hash" / "none"
    bool del(const std::string& key);                                  // Delete a key from all stores
    bool expire(const std::string& key, int seconds);                  // Set a time-to-live on a key
    void purgeExpired();                                               // Drop keys whose TTL has passed
    bool rename(const std::string& oldKey, const std::string& newKey); // Move a key (and its TTL) to a new name

    // List Operations
    std::vector<std::string> lget(const std::string& key);                 // Return the whole list
    ssize_t llen(const std::string& key);                                  // Number of elements in the list
    void lpush(const std::string& key, const std::string& value);          // Prepend to the list (left/head)
    void rpush(const std::string& key, const std::string& value);          // Append to the list (right/tail)
    bool lpop(const std::string& key, std::string& value);                 // Remove & return the head element
    bool rpop(const std::string& key, std::string& value);                 // Remove & return the tail element
    int lrem(const std::string& key, int count, const std::string& value); // Remove matching elements (see .cpp)
    bool lindex(const std::string& key, int index, std::string& value); // Read element at index (negative = from end)
    bool lset(const std::string& key, int index, const std::string& value); // Overwrite element at index

    // Hash Operations
    bool hset(const std::string& key, const std::string& field, const std::string& value); // Set one field
    bool hget(const std::string& key, const std::string& field, std::string& value);       // Read one field
    bool hexists(const std::string& key, const std::string& field);                        // Does the field exist?
    bool hdel(const std::string& key, const std::string& field);                           // Remove one field
    std::unordered_map<std::string, std::string> hgetall(const std::string& key);          // All field/value pairs
    std::vector<std::string> hkeys(const std::string& key);                                // All field names
    std::vector<std::string> hvals(const std::string& key);                                // All field values
    ssize_t hlen(const std::string& key);                                                  // Number of fields
    bool hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fieldValues); // Bulk set

    // Persistence: Dump / load the database from a file.
    bool dump(const std::string& filename); // Write the whole DB to a text file
    bool load(const std::string& filename); // Replace the DB with the contents of a file

  private:
    // Singleton plumbing: the constructor/destructor are private and the
    // copy operations are deleted so the only way to get an instance is
    // through getInstance(), guaranteeing a single shared store.
    RedisDatabase() = default;
    ~RedisDatabase() = default;
    RedisDatabase(const RedisDatabase&) = delete;
    RedisDatabase& operator=(const RedisDatabase&) = delete;

    // Internal helpers. These do NOT lock db_mutex; callers must already hold it.
    bool keyExists(const std::string& key) const;    // True if the key lives in any store
    void eraseFromAllStores(const std::string& key); // Remove the key from every store

    std::mutex db_mutex; // Guards all four maps below against concurrent access

    // The actual data, partitioned by value type.
    std::unordered_map<std::string, std::string> kv_store;                                    // string values
    std::unordered_map<std::string, std::vector<std::string>> list_store;                     // list values
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> hash_store; // hash values

    // For keys with a TTL: maps key -> the time point at which it expires.
    // Keys not present here never expire.
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry_map;
};

#endif
