package io.db.inspector

import android.content.Context
import io.paperdb.Book
import io.paperdb.Paper
import java.io.File

class PaperDbInspector(context:Context, private val dbName:String?) {
    private val book:Book
    init {
        Paper.init(context)
        book = if (dbName != null) Paper.book(dbName) else Paper.book()
    }
    fun listAllKey():HashMap<String, Pair<File, String>> {
        val hashMap = HashMap<String, Pair<File, String>>()
        val path = book().path
        val allKeys = book.allKeys
        for (key in allKeys){
            hashMap[key] = Pair(File("$path/$key"), "")
        }
        return hashMap
    }


    fun dataByKey(key:String):Any? {
        val any = book().read<Any>(key)
        return any
    }

    private fun book():Book{
        val dbName = this.dbName
        if (dbName != null) return Paper.book(dbName)
        return Paper.book()
    }
}