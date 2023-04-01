package io.db.inspector

import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets


object ConverterUtils {
    private const val MAX_BLOB_LENGTH = 512
    private const val UNKNOWN_BLOB_LABEL = "{blob}"
    fun blobToString(blob: ByteArray): String {
        if (blob.size <= MAX_BLOB_LENGTH) {
            if (fastIsAscii(blob)) {
                try {
                    return String(blob, StandardCharsets.US_ASCII)
                } catch (ignored: UnsupportedEncodingException) {
                }
            }
        }
        return UNKNOWN_BLOB_LABEL
    }

    fun fastIsAscii(blob: ByteArray): Boolean {
        for (b in blob) {
            if (b.toInt() and 0x7f.inv() != 0) {
                return false
            }
        }
        return true
    }
}
