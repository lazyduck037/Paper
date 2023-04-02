package io.db.inspector

import android.content.Context
import android.net.Uri
import android.text.TextUtils
import com.google.gson.Gson
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.PrintStream
import java.net.Socket

internal val APP_SHARED_PREFERENCES = "APP_SHARED_PREFERENCES";
internal val PAPER_DB = "APP_PAPER_DB";
class RequestHandler(
    val context: Context,
    private val gson: Gson,
    private val paper:PaperDbInspector
){

    private val customDatabaseFiles = HashMap<String, Triple<File, String, String>>()
    private var databaseFiles: HashMap<String, Triple<File, String,String>>? = null
    private var sqLiteDB:SQLiteDB? = null
    private var isDbOpened = false

    private var dbFactory:DBFactory = DebugDBFactory()
    private var selectedDatabase: String? = null
    fun handle(socket: Socket) {
        var output:PrintStream? = null
        var reader:BufferedReader? = null
        try {
            reader = BufferedReader(InputStreamReader(socket.getInputStream()));
            var line = reader.readLine()
            var route = ""
            while (!TextUtils.isEmpty(line)) {
                if (line.startsWith("GET /")) {
                    val start = line.indexOf('/') + 1
                    val end = line.indexOf(' ', start)
                    route = line.substring(start, end)
                    break
                }
                line = reader.readLine()
            }

            if (route.isEmpty()) {
                route = "index.html";
            }

            val bytes = if (route.startsWith("getDbList")){
                getDBListResponse()?.toByteArray() ?: SERVER_ERROR
            } else if(route.startsWith("getTableList")) {
                getTableListResponse(route)?.toByteArray() ?: SERVER_ERROR
            }else if(route.startsWith("getAllDataFromTheTable")){
                getAllDataFromTheTableResponse(route)?.toByteArray() ?: SERVER_ERROR
            }
            else{
                route.loadFileAsset(context.assets)
            }
            output = PrintStream(socket.getOutputStream())
            if (bytes == null){
                output.println("HTTP/1.0 500 Internal Server Error")
                output.flush()
            }else {
                output.println("HTTP/1.0 200 OK");
                output.println("Content-Type: " + route.toMimeType())
                output.println("Content-Length: " + bytes.size)

                output.println()
                output.write(bytes)
                output.flush()
            }
        } finally {
            try {
                output?.close()
                reader?.close()
            }catch (e:Exception){
                e.printStackTrace()
            }
        }
    }

    private fun getDBListResponse():String? {
        val sqlDbAndShare: java.util.HashMap<String, Triple<File, String,String>> = context.databaseFiles()
        val paperDb: java.util.HashMap<String, Triple<File, String, String>> = hashMapOf(paper.dbName() to Triple(File(paper.getPathDb()), "", "nosql"))

        sqlDbAndShare.putAll(paperDb)
        databaseFiles = sqlDbAndShare

        val databaseFiles = databaseFiles ?: return null
        customDatabaseFiles.putAll(databaseFiles)

        val response =  Response()
        for (entry in databaseFiles) {
            val dbEntry = arrayOf(entry.key, if (entry.value.second == "") "false" else "true", "true", entry.value.third)
            response.rows.add(dbEntry)
        }

        response.rows.add(arrayOf(APP_SHARED_PREFERENCES, "false", "false", "pref"))
        response.isSuccessful = true
        return gson.toJson(response)
    }

    private fun getTableListResponse(route:String):String? {
        val uri = Uri.parse(route)
        val database = uri.getQueryParameter("database") ?: return null
        val typeDb = uri.getQueryParameter("typeDb") ?: return null
        val response = if (APP_SHARED_PREFERENCES == database) {
            val res = context.getAllPrefTableName()
            closeDatabase()
            selectedDatabase = APP_SHARED_PREFERENCES;
            res
        } else {
            if (typeDb == "sql") {
                openDatabase(database)
                val sqLiteDB = sqLiteDB ?: return null
                val res = DatabaseHelper.getAllTableName(sqLiteDB)
                selectedDatabase = database
                res
            }else if(typeDb == "nosql"){
                val res = paper.listAllKey()
                selectedDatabase = database
                res
            }else{
                null
            } ?: return null
        }

        return gson.toJson(response);
    }

    private fun getAllDataFromTheTableResponse(route:String):String? {
        val uri = Uri.parse(route)
        val tableName = uri.getQueryParameter("tableName") ?: return null
        val database = uri.getQueryParameter("database") ?: return null
        val typeDb = uri.getQueryParameter("typeDb") ?: return null

        val response = if (typeDb == "nosql" && database == selectedDatabase) {
                paper.getData(tableName, gson)
            } else if (typeDb == "sql" && isDbOpened) {
                val sql = "SELECT * FROM $tableName"
                val sqLiteDB = sqLiteDB ?: return null
                DatabaseHelper.getTableData(sqLiteDB, sql, tableName);
            } else {
                context.getAllPrefData(tableName);
            }

        return gson.toJson(response)
    }

    private fun openDatabase(database: String) {
        closeDatabase()
        val databaseFiles = databaseFiles ?: return
        val databaseFile: File = databaseFiles[database]?.first ?: return
        val password: String = databaseFiles[database]?.second ?: return
        sqLiteDB = dbFactory.create(context, databaseFile.absolutePath, password)
        isDbOpened = true
    }

    private fun closeDatabase() {
        val sqLiteDB = sqLiteDB
        if (sqLiteDB != null && sqLiteDB.isOpen) {
            sqLiteDB.close()
        }
        this.sqLiteDB = null
        isDbOpened = false
    }
}