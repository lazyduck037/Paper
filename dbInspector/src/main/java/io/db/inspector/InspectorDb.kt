package io.db.inspector

object InspectorDb {
    fun init(port:Int? = null, dbName:String? = null){

        Server(port ?: 8080, dbName).start()
    }
}