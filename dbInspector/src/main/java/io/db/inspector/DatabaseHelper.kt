package io.db.inspector

import android.content.ContentValues
import android.database.Cursor
import android.text.TextUtils

object DatabaseHelper {
    fun getAllTableName(database: SQLiteDB): Response {
        val response = Response()
        val c = database.rawQuery(
            "SELECT name FROM sqlite_master WHERE type='table' OR type='view' ORDER BY name COLLATE NOCASE",
            null
        )
        if (c!!.moveToFirst()) {
            while (!c.isAfterLast) {
                response.rows.add(c.getString(0))
                c.moveToNext()
            }
        }
        c.close()
        response.isSuccessful = true
        try {
            response.dbVersion = database.version
        } catch (ignore: Exception) {
        }
        return response
    }

    fun getTableData(db: SQLiteDB, selectQuery: String, tableName: String?): TableDataResponse {
        var selectQuery = selectQuery
        var tableName = tableName
        val tableData = TableDataResponse()
        tableData.isSelectQuery = true
        if (tableName == null) {
            tableName = getTableName(selectQuery)
        }
        val quotedTableName = getQuotedTableName(tableName)
        if (tableName != null) {
            val pragmaQuery = "PRAGMA table_info($quotedTableName)"
            tableData.tableInfos = getTableInfo(db, pragmaQuery)
        }
        var cursor: Cursor? = null
        var isView = false
        try {
            cursor =
                db.rawQuery("SELECT type FROM sqlite_master WHERE name=?", arrayOf(quotedTableName))
            if (cursor!!.moveToFirst()) {
                isView = "view".equals(cursor.getString(0), ignoreCase = true)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            cursor?.close()
        }
        tableData.isEditable = tableName != null && tableData.tableInfos != null && !isView
        if (!TextUtils.isEmpty(tableName)) {
            selectQuery = selectQuery.replace(tableName!!, quotedTableName)
        }
        try {
            cursor = db.rawQuery(selectQuery, null)
        } catch (e: Exception) {
            e.printStackTrace()
            tableData.isSuccessful = false
            tableData.errorMessage = e.message
            return tableData
        }
        return if (cursor != null) {
            cursor.moveToFirst()

            // setting tableInfo when tableName is not known and making
            // it non-editable also by making isPrimary true for all
            if (tableData.tableInfos == null) {
                tableData.tableInfos = ArrayList()
                for (i in 0 until cursor.columnCount) {
                    val tableInfo = TableDataResponse.TableInfo()
                    tableInfo.title = cursor.getColumnName(i)
                    tableInfo.isPrimary = true
                    tableData.tableInfos!!.add(tableInfo)
                }
            }
            tableData.isSuccessful = true
            tableData.rows = ArrayList<Any>()
            val columnNames = cursor.columnNames
            val tableInfoListModified = ArrayList<TableDataResponse.TableInfo>()
            for (columnName in columnNames) {
                for (tableInfo in tableData.tableInfos!!) {
                    if (columnName == tableInfo.title) {
                        tableInfoListModified.add(tableInfo)
                        break
                    }
                }
            }
            if (tableInfoListModified != null && tableData.tableInfos?.size !== tableInfoListModified.size) {
                tableData.tableInfos = tableInfoListModified
                tableData.isEditable = false
            }
            if (cursor.count > 0) {
                do {
                    val row: MutableList<TableDataResponse.ColumnData> =
                        ArrayList<TableDataResponse.ColumnData>()
                    for (i in 0 until cursor.columnCount) {
                        val columnData: TableDataResponse.ColumnData =
                            TableDataResponse.ColumnData()
                        when (cursor.getType(i)) {
                            Cursor.FIELD_TYPE_BLOB -> {
                                columnData.dataType = DataType.TEXT
                                columnData.value = ConverterUtils.blobToString(cursor.getBlob(i))
                            }
                            Cursor.FIELD_TYPE_FLOAT -> {
                                columnData.dataType = DataType.REAL
                                columnData.value = cursor.getDouble(i)
                            }
                            Cursor.FIELD_TYPE_INTEGER -> {
                                columnData.dataType = DataType.INTEGER
                                columnData.value = cursor.getLong(i)
                            }
                            Cursor.FIELD_TYPE_STRING -> {
                                columnData.dataType = DataType.TEXT
                                columnData.value = cursor.getString(i)
                            }
                            else -> {
                                columnData.dataType = DataType.TEXT
                                columnData.value = cursor.getString(i)
                            }
                        }
                        row.add(columnData)
                    }
                    tableData.rows!!.add(row)
                } while (cursor.moveToNext())
            }
            cursor.close()
            tableData
        } else {
            tableData.isSuccessful = false
            tableData.errorMessage = "Cursor is null"
            tableData
        }
    }

    private fun getQuotedTableName(tableName: String?): String {
        return String.format("[%s]", tableName)
    }

    private fun getTableInfo(db: SQLiteDB, pragmaQuery: String): ArrayList<TableDataResponse.TableInfo>? {
        val cursor: Cursor? = try {
            db.rawQuery(pragmaQuery, null)
        } catch (e: Exception) {
            e.printStackTrace()
            return null
        }
        if (cursor != null) {
            val tableInfoList = ArrayList<TableDataResponse.TableInfo>()
            cursor.moveToFirst()
            if (cursor.count > 0) {
                do {
                    val tableInfo = TableDataResponse.TableInfo()
                    for (i in 0 until cursor.columnCount) {
                        when (cursor.getColumnName(i)) {
                            Constants.PK -> tableInfo.isPrimary = cursor.getInt(i) == 1
                            Constants.NAME -> tableInfo.title = cursor.getString(i)
                            else -> {}
                        }
                    }
                    tableInfoList.add(tableInfo)
                } while (cursor.moveToNext())
            }
            cursor.close()
            return tableInfoList
        }
        return null
    }

    fun addRow(
        db: SQLiteDB, tableName: String?,
        rowDataRequests: List<RowDataRequest?>?
    ): UpdateRowResponse {
        var tableName = tableName
        val updateRowResponse = UpdateRowResponse()
        if (rowDataRequests == null || tableName == null) {
            updateRowResponse.isSuccessful = false
            return updateRowResponse
        }
        tableName = getQuotedTableName(tableName)
        val contentValues = ContentValues()
        for (rowDataRequest in rowDataRequests) {
            if (Constants.NULL == rowDataRequest?.value) {
                rowDataRequest.value = null
            }
            when (rowDataRequest?.dataType) {
                DataType.INTEGER -> contentValues.put(
                    rowDataRequest.title,
                    java.lang.Long.valueOf(rowDataRequest.value)
                )
                DataType.REAL -> contentValues.put(
                    rowDataRequest.title,
                    java.lang.Double.valueOf(rowDataRequest.value)
                )
                DataType.TEXT -> contentValues.put(rowDataRequest.title, rowDataRequest.value)
                else -> contentValues.put(rowDataRequest?.title, rowDataRequest?.value)
            }
        }
        val result = db.insert(tableName, null, contentValues)
        updateRowResponse.isSuccessful = result > 0
        return updateRowResponse
    }

    fun updateRow(
        db: SQLiteDB,
        tableName: String?,
        rowDataRequests: List<RowDataRequest?>?
    ): UpdateRowResponse {
        var tableName = tableName
        val updateRowResponse = UpdateRowResponse()
        if (rowDataRequests == null || tableName == null) {
            updateRowResponse.isSuccessful = false
            return updateRowResponse
        }
        tableName = getQuotedTableName(tableName)
        val contentValues = ContentValues()
        var whereClause: String? = null
        val whereArgsList: MutableList<String> = ArrayList()
        for (rowDataRequest in rowDataRequests) {
            if (Constants.NULL == rowDataRequest?.value) {
                rowDataRequest.value = null
            }
            if (rowDataRequest?.isPrimary == true) {
                whereClause = if (whereClause == null) {
                    rowDataRequest.title + "=? "
                } else {
                    whereClause + "and " + rowDataRequest.title + "=? "
                }
                rowDataRequest.value?.let { whereArgsList.add(it) }
            } else {
                when (rowDataRequest?.dataType) {
                    DataType.INTEGER -> contentValues.put(
                        rowDataRequest.title,
                        java.lang.Long.valueOf(rowDataRequest.value)
                    )
                    DataType.REAL -> contentValues.put(
                        rowDataRequest.title,
                        java.lang.Double.valueOf(rowDataRequest.value)
                    )
                    DataType.TEXT -> contentValues.put(rowDataRequest.title, rowDataRequest.value)
                    else -> {}
                }
            }
        }
        val whereArgs = arrayOfNulls<String>(whereArgsList.size)
        for (i in whereArgsList.indices) {
            whereArgs[i] = whereArgsList[i]
        }
        db.update(tableName, contentValues, whereClause, whereArgs)
        updateRowResponse.isSuccessful = true
        return updateRowResponse
    }

    fun deleteRow(
        db: SQLiteDB, tableName: String?,
        rowDataRequests: List<RowDataRequest?>?
    ): UpdateRowResponse {
        var tableName = tableName
        val updateRowResponse = UpdateRowResponse()
        if (rowDataRequests == null || tableName == null) {
            updateRowResponse.isSuccessful = false
            return updateRowResponse
        }
        tableName = getQuotedTableName(tableName)
        var whereClause: String? = null
        val whereArgsList: MutableList<String> = ArrayList()
        for (rowDataRequest in rowDataRequests) {
            if (Constants.NULL == rowDataRequest?.value) {
                rowDataRequest.value = null
            }
            if (rowDataRequest?.isPrimary == true) {
                whereClause = if (whereClause == null) {
                    rowDataRequest.title + "=? "
                } else {
                    whereClause + "and " + rowDataRequest.title + "=? "
                }
                if (rowDataRequest.value != null) {
                    whereArgsList.add(rowDataRequest.value!!)
                }
            }
        }
        if (whereArgsList.size == 0) {
            updateRowResponse.isSuccessful = true
            return updateRowResponse
        }
        val whereArgs = arrayOfNulls<String>(whereArgsList.size)
        for (i in whereArgsList.indices) {
            whereArgs[i] = whereArgsList[i]
        }
        db.delete(tableName, whereClause, whereArgs)
        updateRowResponse.isSuccessful = true
        return updateRowResponse
    }

    fun exec(database: SQLiteDB, sql: String): TableDataResponse {
        var sql = sql
        val tableDataResponse = TableDataResponse()
        tableDataResponse.isSelectQuery = false
        try {
            val tableName = getTableName(sql)
            if (!TextUtils.isEmpty(tableName)) {
                val quotedTableName = getQuotedTableName(tableName)
                sql = sql.replace(tableName!!, quotedTableName)
            }
            database.execSQL(sql)
        } catch (e: Exception) {
            e.printStackTrace()
            tableDataResponse.isSuccessful = false
            tableDataResponse.errorMessage = e.message
            return tableDataResponse
        }
        tableDataResponse.isSuccessful = true
        return tableDataResponse
    }

    private fun getTableName(selectQuery: String): String? {
        // TODO: Handle JOIN Query
        val tableNameParser = TableNameParser(selectQuery)
        val tableNames = tableNameParser.tables() as HashSet<String>
        for (tableName in tableNames) {
            if (!TextUtils.isEmpty(tableName)) {
                return tableName
            }
        }
        return null
    }
}
