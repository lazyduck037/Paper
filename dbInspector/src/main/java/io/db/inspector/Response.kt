package io.db.inspector


class Response {
    var rows = ArrayList<Any>()
    var columns = ArrayList<String>()
    var isSuccessful = false
    var error:String? = null
    var dbVersion = 0
}