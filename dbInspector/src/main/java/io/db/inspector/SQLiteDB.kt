package io.db.inspector

import android.content.ContentValues
import android.database.Cursor
import android.database.SQLException

interface SQLiteDB {
    fun delete(table: String?, whereClause: String?, whereArgs: Array<String?>?): Int
    val isOpen: Boolean
    fun close()
    fun rawQuery(sql: String?, selectionArgs: Array<String?>?): Cursor?

    @Throws(SQLException::class)
    fun execSQL(sql: String?)

    fun insert(table: String?, nullColumnHack: String?, values: ContentValues?): Long
    fun update(table: String?, values: ContentValues?, whereClause: String?, whereArgs: Array<String?>?): Int

    val version: Int
}
