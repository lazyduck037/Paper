package io.db.inspector

import android.text.TextUtils
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintStream
import java.net.Socket

class Request {
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
            val bytes = "Hello".toByteArray()
            output = PrintStream(socket.getOutputStream())
            output.println("HTTP/1.0 200 OK");
            output.println("Content-Type: " + route.toMimeType())
            output.println("Content-Length: " + bytes.size)

            output.println()
            output.write(bytes)
            output.flush()
        } finally {
            try {
                output?.close()
                reader?.close()
            }catch (e:Exception){
                e.printStackTrace()
            }
        }
    }
}