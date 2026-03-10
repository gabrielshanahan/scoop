package io.github.gabrielshanahan.scoop.util

import java.security.SecureRandom
import java.util.UUID

private val random = SecureRandom()

/** Generates a UUIDv7 (Unix Epoch time-based, with random bits). */
@Suppress("MagicNumber")
fun uuidV7(): UUID {
    val timestamp = System.currentTimeMillis()
    val randomBytes = ByteArray(10)
    random.nextBytes(randomBytes)

    // Most significant 64 bits: 48-bit timestamp | 4-bit version (0111) | 12-bit random
    val msb =
        (timestamp shl 16) or // 48-bit timestamp in upper bits
            (0x7000L) or // version 7
            ((randomBytes[0].toLong() and 0x0F) shl 8) or
            (randomBytes[1].toLong() and 0xFF)

    // Least significant 64 bits: 2-bit variant (10) | 62-bit random
    val lsb =
        ((randomBytes[2].toLong() and 0x3F) or 0x80.toLong() shl 56) or
            ((randomBytes[3].toLong() and 0xFF) shl 48) or
            ((randomBytes[4].toLong() and 0xFF) shl 40) or
            ((randomBytes[5].toLong() and 0xFF) shl 32) or
            ((randomBytes[6].toLong() and 0xFF) shl 24) or
            ((randomBytes[7].toLong() and 0xFF) shl 16) or
            ((randomBytes[8].toLong() and 0xFF) shl 8) or
            (randomBytes[9].toLong() and 0xFF)

    return UUID(msb, lsb)
}
