package io.db.inspector

import android.content.Context
import com.google.gson.Gson
import io.paperdb.Book
import io.paperdb.Paper
import java.util.ArrayList

class PaperDbInspector(context:Context, private val dbName:String?) {
    private val book:Book
    init {
        Paper.init(context)
        book = if (dbName != null) Paper.book(dbName) else Paper.book()
    }

    fun dbName():String = dbName ?: book.dbName

    fun getPathDb():String = book.path

    fun listAllKey():Response {
        val response = Response()
        val keys = book.allKeys.toMuteListIfNeed()
        for (key in keys) {
            response.rows.add(key)
        }

        response.isSuccessful = true
        return response
    }

    fun getData(key: String, gson: Gson): TableDataResponse? {

        val response = TableDataResponse()
        response.isEditable = true
        response.isSuccessful = true
        response.isSelectQuery = true

        response.rows = ArrayList<Any>()
        val data = book.read<Any>(key) ?: return null

        when (data) {
            is Collection<*> -> {
                response.tableInfos = valueFieldInTable()
                for (item in data) {
                    val row = ArrayList<Any>()
                    row.add(dataField(item, gson))
                    response.rows.add(row)
                }
                return response
            }
            is Map<*,*> -> {
                response.tableInfos = keyValueInTable()
                for (entry in data) {
                    val row = ArrayList<Any>()

                    row.add(dataField(entry.key, gson))
                    row.add(dataField(entry.value, gson))

                    response.rows.add(row)
                }
                return response
            }
            else -> {
                response.tableInfos = valueFieldInTable()

                val row = ArrayList<Any>()
                row.add(dataField(data, gson));
                response.rows.add(row);

                return response
            }
        }
    }

    private fun valueFieldInTable(): ArrayList<TableDataResponse.TableInfo> {
        val valueInfo = TableDataResponse.TableInfo()
        valueInfo.isPrimary = false
        valueInfo.title = "Value"

        val tableInfos = ArrayList<TableDataResponse.TableInfo>()
        tableInfos.add(valueInfo)
        return tableInfos
    }

    private fun keyValueInTable(): ArrayList<TableDataResponse.TableInfo> {
        val keyInfo = TableDataResponse.TableInfo()
        keyInfo.isPrimary = true
        keyInfo.title = "Key"

        val valueInfo = TableDataResponse.TableInfo()
        valueInfo.isPrimary = false
        valueInfo.title = "Value"

        val tableInfos = ArrayList<TableDataResponse.TableInfo>()
        tableInfos.add(keyInfo)
        tableInfos.add(valueInfo)
        return tableInfos
    }

    private fun dataField(data:Any?, gson:Gson):TableDataResponse.ColumnData{
        val valueColumnData = TableDataResponse.ColumnData()
        if (data == null){
            valueColumnData.dataType = DataType.TEXT
            valueColumnData.value = "null"
            return valueColumnData
        }
        when (data) {
            is String -> {
                valueColumnData.dataType = DataType.TEXT
                valueColumnData.value = gson.toJson(data)
            }
            is Int -> {
                valueColumnData.dataType = DataType.INTEGER
                valueColumnData.value = data
            }
            is Long -> {
                valueColumnData.dataType = DataType.LONG
                valueColumnData.value = data
            }
            is Float -> {
                valueColumnData.dataType = DataType.FLOAT
                valueColumnData.value = data
            }
            is Boolean -> {
                valueColumnData.dataType = DataType.BOOLEAN
                valueColumnData.value = data
            }
            is Set<*> -> {
                valueColumnData.dataType = DataType.STRING_SET
                valueColumnData.value = data
            }
            else ->{
                valueColumnData.dataType = DataType.TEXT
                valueColumnData.value = gson.toJson(data)
            }
        }
        return valueColumnData
    }

}