package io.db.inspector

import android.content.Context
import com.google.gson.Gson

object InspectorDb {
    fun init(context: Context, gson: Gson? = null,port: Int? = null, dbName: String? = null) {
        Server(context, gson,port ?: 8080, dbName).start()
    }
}