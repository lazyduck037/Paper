package io.db.inspector

import android.content.Context
import android.text.TextUtils
import com.google.gson.Gson
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.PrintStream
import java.net.Socket

internal val APP_SHARED_PREFERENCES = "APP_SHARED_PREFERENCES";
class RequestHandler(val context: Context, val gson: Gson, val paper:PaperDbInspector){

    private val customDatabaseFiles = HashMap<String, Pair<File, String>>()
    private var databaseFiles: HashMap<String, Pair<File, String>>? = null
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
                getDBListResponse().toByteArray()
            } else if(route.startsWith("getTableList")) {
                getTableListResponse(route).toByteArray()
            }else{
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

    private fun getDBListResponse():String {
        val sqlDbAndShare: java.util.HashMap<String, Pair<File, String>> = context.databaseFiles()
        val paperDb: java.util.HashMap<String, Pair<File, String>> = paper.listAllKey()

        sqlDbAndShare.putAll(paperDb)
        databaseFiles = sqlDbAndShare

        val databaseFiles = databaseFiles ?: return ""
        customDatabaseFiles.putAll(databaseFiles)

        val response =  Response()
        for (entry in databaseFiles) {
            val dbEntry = arrayOf(entry.key, if (entry.value.second == "") "false" else "true", "true")
            response.rows.add(dbEntry)
        }

        response.rows.add(arrayOf(APP_SHARED_PREFERENCES, "false", "false"));
        response.isSuccessful = true;
        return gson.toJson(response);
    }

    private fun getTableListResponse(route:String):String {
        val database = if (route.contains("?database=")) {
            route.substring(route.indexOf("=") + 1, route.length)
        } else null

         var response:Response? = null

        if (APP_SHARED_PREFERENCES == database) {
            response = context.getAllPrefTableName()
            closeDatabase()
            selectedDatabase = APP_SHARED_PREFERENCES;
        } else if (database != null){
            openDatabase(database)
            if (sqLiteDB != null) {
                response = DatabaseHelper.getAllTableName(sqLiteDB!!)
                selectedDatabase = database
            }
        }
        return gson.toJson(response);
    }

    private fun openDatabase(database: String) {
        closeDatabase()
        val databaseFiles = databaseFiles ?: return
        val databaseFile: File = databaseFiles[database]?.first ?: return
        val password: String = databaseFiles.get(database)?.second ?: return
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