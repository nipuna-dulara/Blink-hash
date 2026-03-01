#include "wal_record.h"
#include <cstring>

namespace BLINK_HASH {
namespace WAL {

/* CRC-16/ARC (IBM)*/

/*
 * Standard CRC-16/ARC — polynomial 0x8005, bit-at-a-time.
 * Adequate for corruption detection; not intended to be cryptographic.
 * Hot path is typically < 256 bytes, so a table-driven version
 * is overkill (and wastes 512 B in L1d).
 */
uint16_t wal_crc16(const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint16_t crc = 0;
    for (size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint16_t>(p[i]);
        for (int bit = 0; bit < 8; ++bit) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xA001;   /* reflected 0x8005 */
            else
                crc >>= 1;
        }
    }
    return crc;
}

/* serialize */

size_t wal_record_serialize(RecordType type,
                            uint64_t   lsn,
                            const void* payload,
                            size_t      payload_size,
                            void*       dst)
{
    const size_t total = sizeof(RecordHeader) + payload_size;

    RecordHeader hdr;
    hdr.lsn        = lsn;
    hdr.total_size = static_cast<uint32_t>(total);
    hdr.type       = static_cast<uint16_t>(type);
    hdr.crc16      = wal_crc16(payload, payload_size);

    char* out = static_cast<char*>(dst);
    std::memcpy(out, &hdr, sizeof(RecordHeader));
    if (payload_size > 0)
        std::memcpy(out + sizeof(RecordHeader), payload, payload_size);

    return total;
}

/* deserialize */

const void* wal_record_deserialize(const void*   src,
                                   size_t        available,
                                   RecordHeader* hdr_out)
{
    /* Need at least the header */
    if (available < sizeof(RecordHeader))
        return nullptr;

    std::memcpy(hdr_out, src, sizeof(RecordHeader));

    /* Sanity: total_size must cover at least the header */
    if (hdr_out->total_size < sizeof(RecordHeader))
        return nullptr;

    /* Enough data for the full record? */
    if (available < hdr_out->total_size)
        return nullptr;

    /* Payload sits right after the header */
    const char* payload_ptr =
        static_cast<const char*>(src) + sizeof(RecordHeader);
    size_t payload_size = hdr_out->total_size - sizeof(RecordHeader);

    /* CRC check */
    uint16_t actual = wal_crc16(payload_ptr, payload_size);
    if (actual != hdr_out->crc16)
        return nullptr;

    return payload_ptr;
}

} 
}