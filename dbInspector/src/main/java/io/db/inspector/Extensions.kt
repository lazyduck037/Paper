package io.db.inspector

import android.text.TextUtils


fun String.toMimeType():String?{
    return if (TextUtils.isEmpty(this)) {
        null;
    } else if (endsWith(".html")) {
        "text/html";
    } else if (endsWith(".js")) {
        "application/javascript";
    } else if (endsWith(".css")) {
        "text/css";
    } else {
        "application/octet-stream";
    }
}