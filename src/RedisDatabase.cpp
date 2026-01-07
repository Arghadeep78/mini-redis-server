#include "../include/RedisDatabase.h"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <iterator>

// Singleton accessor.
// The static local is constructed once on first call and shared thereafter;
// C++ guarantees this initialization is thread-safe.
RedisDatabase& RedisDatabase::getInstance() {
    static RedisDatabase instance;
    return instance;
}

// NOTE on locking: nearly every method below takes a lock_guard on db_mutex
// at the top. Because the guard holds the lock for the whole function body and
// releases it automatically on return, only one thread touches the maps at a
// time — that's what makes the shared database safe across client threads.

// Common Comands

// Remove every key from all three stores (string, list, hash) and all TTLs.
bool RedisDatabase::flushAll() {
    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store.clear();
    list_store.clear();
    hash_store.clear();
    expiry_map.clear(); // orphaned TTL entries must not survive a flush
    return true;
}

// Key/Value Operations

// Store (or overwrite) a string value for the given key.
// Clears any prior TTL so a SET never inherits an old expiration.
void RedisDatabase::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store[key] = value;
    expiry_map.erase(key); // a fresh SET resets any previous TTL
}

// Look up a string value. Returns true and fills `value` if found; false otherwise.
bool RedisDatabase::get(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired(); // drop any keys whose TTL elapsed before reading
    auto it = kv_store.find(key);
    if (it != kv_store.end()) {
        value = it->second;
        return true;
    }
    return false;
}

// Collect every key currently stored, across all three value types.
std::vector<std::string> RedisDatabase::keys() {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    for (const auto& pair : kv_store) {
        result.push_back(pair.first);
    }
    for (const auto& pair : list_store) {
        result.push_back(pair.first);
    }
    for (const auto& pair : hash_store) {
        result.push_back(pair.first);
    }
    return result;
}

// Report which store a key lives in, as a type name string.
std::string RedisDatabase::type(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (kv_store.find(key) != kv_store.end())
        return "string";
    if (list_store.find(key) != list_store.end())
        return "list";
    if (hash_store.find(key) != hash_store.end())
        return "hash";
    else return "none";
}

// Delete a key from whichever store(s) hold it and remove any TTL entry.
bool RedisDatabase::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool erased = false;
    erased |= kv_store.erase(key) > 0;   // erase() returns count removed (0 or 1)
    erased |= list_store.erase(key) > 0;
    erased |= hash_store.erase(key) > 0;
    expiry_map.erase(key); // always clean up TTL so re-created keys don't inherit it
    return erased;
}

// Schedule a key to be deleted `seconds` from now. Returns false if the key
// doesn't currently exist in any store.
bool RedisDatabase::expire(const std::string& key, int seconds) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool exists = (kv_store.find(key) != kv_store.end()) ||
                  (list_store.find(key) != list_store.end()) ||
                  (hash_store.find(key) != hash_store.end());
    if (!exists)
        return false;
    
    // Record the absolute time point at which this key should disappear.
    expiry_map[key] = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    return true;
}

// Lazily delete keys whose TTL has passed. Called from read paths so expired
// data is removed before it can be returned. Assumes db_mutex is already held
// by the caller (note: it does NOT lock the mutex itself).
void RedisDatabase::purgeExpired() {
    auto now = std::chrono::steady_clock::now();
    for (auto it = expiry_map.begin(); it != expiry_map.end(); ) {
        if (now > it->second) {
            // TTL elapsed: remove the key from every store and from the
            // expiry map itself. erase() returns the next iterator so the
            // loop stays valid after removal.
            kv_store.erase(it->first);
            list_store.erase(it->first);
            hash_store.erase(it->first);
            it = expiry_map.erase(it);
        } else {
            ++it; // not expired yet; keep it
        }
    }
}

// Move a key to a new name, carrying its value and any TTL with it.
// Returns false if the old key didn't exist in any store.
bool RedisDatabase::rename(const std::string& oldKey, const std::string& newKey) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (oldKey == newKey) {
        // Self-rename: the key must exist; nothing else to do.
        return (kv_store.count(oldKey) || list_store.count(oldKey) || hash_store.count(oldKey));
    }
    // Overwrite the destination: erase newKey from every store so a key of a
    // different type can't survive alongside the renamed key.
    kv_store.erase(newKey);
    list_store.erase(newKey);
    hash_store.erase(newKey);
    expiry_map.erase(newKey);

    bool found = false;

    // Check each store in turn: if the old key is present there, copy its
    // value under the new name and erase the old entry.
    auto itKv = kv_store.find(oldKey);
    if (itKv != kv_store.end()) {
        kv_store[newKey] = itKv->second;
        kv_store.erase(itKv);
        found = true;
    }

    auto itList = list_store.find(oldKey);
    if (itList != list_store.end()) {
        list_store[newKey] = itList->second;
        list_store.erase(itList);
        found = true;
    }

    auto itHash = hash_store.find(oldKey);
    if (itHash != hash_store.end()) {
        hash_store[newKey] = itHash->second;
        hash_store.erase(itHash);
        found = true;
    }

    // Carry over any TTL so the renamed key still expires on schedule.
    auto itExpire = expiry_map.find(oldKey);
    if (itExpire != expiry_map.end()) {
        expiry_map[newKey] = itExpire->second;
        expiry_map.erase(itExpire);
    }

    return found;
}

// List Opreations

// Return a copy of the whole list, or an empty vector if the key doesn't exist.
std::vector<std::string> RedisDatabase::lget(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it != list_store.end()) {
        return it->second;
    }
    return {};
}

// Return the list's length (0 if the key doesn't exist).
ssize_t RedisDatabase::llen(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it != list_store.end())
        return it->second.size();
    return 0;
}

// Prepend a value to the front (head/left) of the list, creating it if needed.
// Clears any prior TTL when creating a brand-new list key.
void RedisDatabase::lpush(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    if (list_store.find(key) == list_store.end())
        expiry_map.erase(key); // new list key must not inherit a stale TTL
    list_store[key].insert(list_store[key].begin(), value);
}

// Append a value to the back (tail/right) of the list, creating it if needed.
// Clears any prior TTL when creating a brand-new list key.
void RedisDatabase::rpush(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    if (list_store.find(key) == list_store.end())
        expiry_map.erase(key); // new list key must not inherit a stale TTL
    list_store[key].push_back(value);
}

// Remove and return the head element. Returns false if the list is missing or empty.
bool RedisDatabase::lpop(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it != list_store.end() && !it->second.empty()) {
        value = it->second.front();
        it->second.erase(it->second.begin());
        return true;
    }
    return false;
}

// Remove and return the tail element. Returns false if the list is missing or empty.
bool RedisDatabase::rpop(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it != list_store.end() && !it->second.empty()) {
        value = it->second.back();
        it->second.pop_back();
        return true;
    }
    return false;
}

// Remove elements equal to `value` from the list and return how many were removed.
//   count == 0 : remove all matches
//   count  > 0 : remove up to `count` matches, scanning head -> tail
//   count  < 0 : remove up to `|count|` matches, scanning tail -> head
int RedisDatabase::lrem(const std::string& key, int count, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    int removed = 0;
    auto it = list_store.find(key);
    if (it == list_store.end())
        return 0;

    auto& lst = it->second;

    if (count == 0) {
        // Remove all occurances.
        // std::remove shuffles non-matching elements to the front and returns
        // the new logical end; everything from there on is then erased.
        auto new_end = std::remove(lst.begin(), lst.end(), value);
        removed = std::distance(new_end, lst.end());
        lst.erase(new_end, lst.end());
    } else if (count > 0) {
        // Remove from head to tail, stopping after `count` removals.
        for (auto iter = lst.begin(); iter != lst.end() && removed < count; ) {
            if (*iter == value) {
                iter = lst.erase(iter); // erase returns iterator to next element
                ++removed;
            } else {
                ++iter;
            }
        }
    } else {
        // Remove from tail to head (count is negative), stopping after |count|.
        // We walk a reverse iterator and convert to a forward iterator to erase,
        // since vector::erase only accepts forward iterators.
        for (auto riter = lst.rbegin(); riter != lst.rend() && removed < (-count); ) {
            if (*riter == value) {
                auto fwdIter = riter.base();
                --fwdIter;                      // .base() points one past riter; step back to the element
                fwdIter = lst.erase(fwdIter);
                ++removed;
                // Rebuild the reverse iterator from the new forward position.
                riter = std::reverse_iterator<std::vector<std::string>::iterator>(fwdIter);
            } else {
                ++riter;
            }
        }
    }
    return removed;
}

// Read the element at `index` into `value`. Negative indices count from the
// end (-1 = last). Returns false if the key is missing or the index is out of range.
bool RedisDatabase::lindex(const std::string& key, int index, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it == list_store.end())
        return false;

    const auto& lst = it->second;
    if (index < 0)
        index = lst.size() + index;     // translate negative index to a real position
    if (index < 0 || index >= static_cast<int>(lst.size()))
        return false;                    // still out of bounds

    value = lst[index];
    return true;
}

// Overwrite the element at `index`. Negative indices count from the end.
// Returns false if the key is missing or the index is out of range.
bool RedisDatabase::lset(const std::string& key, int index, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = list_store.find(key);
    if (it == list_store.end())
        return false;

    auto& lst = it->second;
    if (index < 0)
        index = lst.size() + index;
    if (index < 0 || index >= static_cast<int>(lst.size()))
        return false;

    lst[index] = value;
    return true;
}

// Hash Operations

// Set field -> value inside the hash at `key`, creating the hash if needed.
// Returns true if the field is new, false if it was updated.
// Clears any prior TTL when creating a brand-new hash key.
bool RedisDatabase::hset(const std::string& key, const std::string& field, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    if (hash_store.find(key) == hash_store.end())
        expiry_map.erase(key); // new hash key must not inherit a stale TTL
    bool isNew = (hash_store[key].find(field) == hash_store[key].end());
    hash_store[key][field] = value;
    return isNew;
}

// Read one field's value into `value`. Returns false if the key or field is absent.
bool RedisDatabase::hget(const std::string& key, const std::string& field, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        auto f = it->second.find(field);
        if (f != it->second.end()) {
            value = f->second;
            return true;
        }
    }
    return false;
}

// Return true if `field` exists within the hash at `key`.
bool RedisDatabase::hexists(const std::string& key, const std::string& field) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = hash_store.find(key);
    if (it != hash_store.end())
        return it->second.find(field) != it->second.end();
    return false;
}

// Remove one field from the hash. Returns true if the field was present.
bool RedisDatabase::hdel(const std::string& key, const std::string& field) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = hash_store.find(key);
    if (it != hash_store.end())
        return it->second.erase(field) > 0;
    return false;
}

// Return a copy of the entire field->value map for the key (empty if absent).
std::unordered_map<std::string, std::string> RedisDatabase::hgetall(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    if (hash_store.find(key) != hash_store.end())
        return hash_store[key];
    return {};
}

// Return just the field names of the hash.
std::vector<std::string> RedisDatabase::hkeys(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::vector<std::string> fields;
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        for (const auto& pair: it->second)
            fields.push_back(pair.first);
    }
    return fields;
}

// Return just the field values of the hash.
std::vector<std::string> RedisDatabase::hvals(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::vector<std::string> values;
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        for (const auto& pair: it->second)
            values.push_back(pair.second);
    }
    return values;
}

// Return the number of fields in the hash (0 if the key doesn't exist).
ssize_t RedisDatabase::hlen(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    auto it = hash_store.find(key);
    return (it != hash_store.end()) ? it->second.size() : 0;
}

// Set many field->value pairs on the hash at once.
bool RedisDatabase::hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fieldValues) {
    std::lock_guard<std::mutex> lock(db_mutex);
    for (const auto& pair: fieldValues) {
        hash_store[key][pair.first] = pair.second;
    }
    return true;
}

/*
Very simple text based persistance: each line encodes a record

Memory -> File - dump()
File -> Memory - load()

K = Key Value
L = List
H= Hash
*/
// Write the whole database to a text file, one record per line.
// Each line is prefixed by a type tag so load() knows how to parse it:
//   K <key> <value>                      -> string
//   L <key> <item1> <item2> ...          -> list
//   H <key> <field1>:<val1> <field2>:... -> hash
// Returns false if the file couldn't be opened for writing.
bool RedisDatabase::dump(const std::string& filename) {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs) return false;

    // String key/values.
    for (const auto& kv: kv_store) {
        ofs << "K " << kv.first << " " << kv.second << "\n";
    }
    // Lists: key followed by each element, space-separated.
    for (const auto& kv : list_store) {
        ofs << "L " << kv.first;
        for (const auto& item : kv.second)
            ofs << " " << item;
        ofs << "\n";
    }
    // Hashes: key followed by each field:value pair.
    for (const auto& kv : hash_store) {
        ofs << "H " << kv.first;
        for (const auto& field_val : kv.second)
            ofs << " " << field_val.first << ":" << field_val.second;
        ofs << "\n";
    }
    return true;
    // NOTE: this format is space-delimited, so keys/values containing spaces
    // (or ':' in hash fields) won't round-trip correctly.
}

/*
Key-Value (K)
kv_store["name"] = "Alice";
kv_store["city"] = "Berlin";

List (L)
list_store["fruits"] = {"apple", "banana", "orange"};
list_store["colors"] = {"red", "green", "blue"};

Hash (H)
hash_store["user:100"] = {
    {"name", "Bob"},
    {"age", "30"},
    {"email", "bob@example.com"}
};

hash_store["user:200"] = {
    {"name", "Eve"},
    {"age", "25"},
    {"email", "eve@example.com"}
};
*/
// Replace the in-memory database with the contents of a dump file.
// Reads each line, branches on its type tag, and reconstructs the matching
// store. Returns false if the file can't be opened.
bool RedisDatabase::load(const std::string& filename) {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) return false;

    // Start from a clean slate so a load fully replaces current state.
    kv_store.clear();
    list_store.clear();
    hash_store.clear();

    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        char type;
        iss >> type; // first token on the line is the type tag (K/L/H)
        if (type == 'K') {
            // String record: K <key> <value>
            std::string key, value;
            iss >> key >> value;
            kv_store[key] = value;
        } else if (type == 'L') {
            // List record: L <key> <item> <item> ...
            std::string key;
            iss >> key;
            std::string item;
            std::vector<std::string> list;
            while (iss >> item)
                list.push_back(item);
            list_store[key] = list;
        } else if (type == 'H') {
            // Hash record: H <key> <field>:<value> <field>:<value> ...
            std::string key;
            iss >> key;
            std::unordered_map<std::string, std::string> hash;
            std::string pair;
            while (iss >> pair) {
                // Split each token on the first ':' into field and value.
                auto pos = pair.find(':');
                if (pos != std::string::npos) {
                    std::string field = pair.substr(0, pos);
                    std::string value = pair.substr(pos+1);
                    hash[field] = value;
                }
            }
            hash_store[key] = hash;
        }
    }
    return true;
}
