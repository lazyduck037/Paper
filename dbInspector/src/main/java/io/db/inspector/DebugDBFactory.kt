package io.db.inspector

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import io.db.inspector.DBFactory
import io.db.inspector.DebugSQLiteDB
import io.db.inspector.SQLiteDB

class DebugDBFactory : DBFactory {
    override fun create(context: Context?, path: String, password: String?): SQLiteDB? {
        return DebugSQLiteDB(SQLiteDatabase.openOrCreateDatabase(path, null));
    }
}