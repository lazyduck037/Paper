package io.db.inspector

import android.content.Context

interface DBFactory {
    fun create(context: Context?, path: String, password: String?): SQLiteDB?
}
