package io.db.inspector

import io.paperdb.Book
import io.paperdb.Paper

class PaperDbInspector(private val dbName:String?) {

    fun listAllKey():List<String> {
        val res = dataByKey("o2");
        return book().allKeys.toMuteListIfNeed()
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