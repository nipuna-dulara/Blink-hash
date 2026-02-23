#ifndef BLINK_HASH_BH_KEY_H__
#define BLINK_HASH_BH_KEY_H__

/*
 * bh_key.h — Unified key type definitions for B^link-hash PG integration
 *
 * This header consolidates both integer (uint64) and variable-length string
 * key support.  GenericKey<N> is adapted from the blink-hash-str variant's
 * include/indexkey.h, made self-contained so the library does not depend on
 * external header paths.
 */

#include <cstdint>
#include <cstring>
#include <string>
#include <functional>
#include <ostream>

namespace BLINK_HASH {

/* ─── Fixed-size string key ─────────────────────────────────────────── */

template <std::size_t keySize>
class GenericKey {
public:
    char data[keySize];

    /* ── constructors ── */
    GenericKey()                       { memset(data, 0x00, keySize); }
    GenericKey(int)                    { memset(data, 0x00, keySize); }
    GenericKey(const GenericKey& other){ memcpy(data, other.data, keySize); }

    /* ── assignment ── */
    inline GenericKey& operator=(const GenericKey& other) {
        memcpy(data, other.data, keySize);
        return *this;
    }

    /* ── string import ── */
    inline void setFromString(const std::string& key) {
        memset(data, 0, keySize);
        if (key.size() >= keySize) {
            memcpy(data, key.c_str(), keySize - 1);
            data[keySize - 1] = '\0';
        } else {
            memcpy(data, key.c_str(), key.size());
            data[key.size()] = '\0';
        }
    }

    /* ── raw-bytes import (for PG Datum conversion) ── */
    inline void setFromBytes(const char* src, size_t len) {
        memset(data, 0, keySize);
        size_t copy_len = (len >= keySize) ? keySize - 1 : len;
        memcpy(data, src, copy_len);
        data[copy_len] = '\0';
    }

    /* ── comparisons (strcmp-based) ── */
    inline bool operator< (const GenericKey& o) const { return strcmp(data, o.data) <  0; }
    inline bool operator> (const GenericKey& o) const { return strcmp(data, o.data) >  0; }
    inline bool operator==(const GenericKey& o) const { return strcmp(data, o.data) == 0; }
    inline bool operator!=(const GenericKey& o) const { return !(*this == o); }
    inline bool operator<=(const GenericKey& o) const { return !(*this > o); }
    inline bool operator>=(const GenericKey& o) const { return !(*this < o); }
};

/* ── Functor helpers ── */

template <std::size_t keySize>
struct GenericComparator {
    inline bool operator()(const GenericKey<keySize>& a,
                           const GenericKey<keySize>& b) const {
        return strcmp(a.data, b.data) < 0;
    }
};

template <std::size_t keySize>
struct GenericEqualityChecker {
    inline bool operator()(const GenericKey<keySize>& a,
                           const GenericKey<keySize>& b) const {
        return strcmp(a.data, b.data) == 0;
    }
};

/* ── Default key length for string workloads ── */
constexpr std::size_t BH_DEFAULT_KEY_LENGTH = 32;
using StringKey = GenericKey<BH_DEFAULT_KEY_LENGTH>;

/* ── Stream output (for debug prints) ── */
template <std::size_t keySize>
inline std::ostream& operator<<(std::ostream& os, const GenericKey<keySize>& k) {
    os << k.data;
    return os;
}

} // namespace BLINK_HASH

#endif // BLINK_HASH_BH_KEY_H__
