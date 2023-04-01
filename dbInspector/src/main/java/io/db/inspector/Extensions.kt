package io.db.inspector

import android.content.Context
import android.content.res.AssetManager
import android.text.TextUtils
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileNotFoundException
import java.io.InputStream
import java.text.MessageFormat
import java.util.*

internal fun <E>List<E>.toMuteListIfNeed():MutableList<E> {
    if (this is ArrayList<E>){
        return this
    }
    return toMutableList()
}
internal fun String.toMimeType():String?{
    return if (TextUtils.isEmpty(this)) {
        null;
    } else if (endsWith(".html")) {
        "text/html";
    } else if (endsWith(".js")) {
        "application/javascript";
    } else if (endsWith(".css")) {
        "text/css";
    } else {
        "application/octet-stream";
    }
}

internal fun String.loadFileAsset(assetManager: AssetManager):ByteArray? {
    var input: InputStream? = null;
    try {
        val output = ByteArrayOutputStream()
        input = assetManager.open(this)
        val buffer = ByteArray(1024)
        var size = input.read(buffer)
        while (-1 != size) {
            output.write(buffer, 0, size)
            size = input.read(buffer)
        }
        output.flush()
        return output.toByteArray()
    } catch (e: FileNotFoundException) {
        return null;
    } finally {
        try {
            input?.close()
        } catch (e:Exception) {
            e.printStackTrace()
        }
    }
}

fun Context.databaseFiles():HashMap<String, Pair<File, String>>{
    val databaseFiles = HashMap<String, Pair<File, String>>()
    try {
        val list =  databaseList()
        for (databaseName in list) {
            val password = getDbPasswordFromStringResources(databaseName);
            databaseFiles[databaseName] = Pair(this.getDatabasePath(databaseName), password);
        }
    } catch (e:Exception) {
        e.printStackTrace();
    }
    return databaseFiles;
}

fun Context.getDbPasswordFromStringResources(name:String):String {
    val nameWithoutExt = if (name.endsWith(".db")) name.substring(0, name.lastIndexOf('.')) else name

    val resourceName = MessageFormat.format(
        "DB_PASSWORD_{0}", nameWithoutExt.uppercase(
            Locale.getDefault()
        )
    )


    val resourceId: Int = resources.getIdentifier(resourceName, "string", packageName)

    return if (resourceId != 0) {
        getString(resourceId)
    } else ""

}

fun Context.getAllPrefTableName():Response {
    val response = Response()

    val prefTags = getSharedPreferenceTags()

    for (tag in prefTags) {
        response.rows.add(tag);
    }

    response.isSuccessful = true;

    return response;
}

fun Context.getSharedPreferenceTags():List<String> {
    val tags = ArrayList<String>()
    val PREFS_SUFFIX = ".xml";
    val rootPath = applicationInfo.dataDir + "/shared_prefs"
    val root = File(rootPath);
    if (root.exists()) {
        val listFiles = root.listFiles() ?: return emptyList()
        for (file in listFiles) {
            val fileName = file.name;
            if (fileName.endsWith(PREFS_SUFFIX)) {
                tags.add(fileName.substring(0, fileName.length - PREFS_SUFFIX.length))
            }
        }
    }

    tags.sort()

    return tags
}