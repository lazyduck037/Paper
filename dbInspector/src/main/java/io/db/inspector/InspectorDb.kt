package io.db.inspector

object InspectorDb {
    fun init(){
        Server(8080).start()
    }
}