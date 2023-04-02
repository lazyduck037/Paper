package io.db.inspector

class TableDataResponse {
    var tableInfos = ArrayList<TableInfo>()
    var isSuccessful = false
    var rows = ArrayList<Any>()
    var errorMessage: String? = null
    var isEditable = false
    var isSelectQuery = false

    class TableInfo {
        var title: String? = null
        var isPrimary = false
    }

    class ColumnData {
        var dataType: String? = null
        var value: Any? = null
    }
}
