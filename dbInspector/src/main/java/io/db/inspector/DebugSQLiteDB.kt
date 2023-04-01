package io.db.inspector

import android.content.ContentValues
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase

class DebugSQLiteDB(val database: SQLiteDatabase) : SQLiteDB {
    override fun delete(table: String?, whereClause: String?, whereArgs: Array<String?>?): Int {
        return database.delete(table, whereClause, whereArgs)
    }

    override val isOpen: Boolean = database.isOpen

    override fun close() = database.close()

    override fun rawQuery(sql: String?, selectionArgs: Array<String?>?): Cursor? {
       return database.rawQuery(sql, selectionArgs)
    }

    override fun execSQL(sql: String?) {
        database.execSQL(sql)
    }

    override fun insert(table: String?, nullColumnHack: String?, values: ContentValues?): Long {
        return database.insert(table, nullColumnHack, values)
    }

    override fun update(
        table: String?,
        values: ContentValues?,
        whereClause: String?,
        whereArgs: Array<String?>?
    ): Int {
        return database.update(table, values, whereClause, whereArgs)
    }

    override val version: Int get() = database.version
}