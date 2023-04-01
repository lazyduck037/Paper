package io.db.inspector

import android.content.Context
import com.google.gson.Gson
import java.net.ServerSocket
import java.net.SocketException

class Server(
    context:Context,
    gson:Gson?,
    private val port:Int,
    dbName:String?
) : Runnable {
    private var mServerSocket: ServerSocket? = null
    private var mIsRunning = false
    private val request = RequestHandler(context,gson ?: Gson() , PaperDbInspector(context, dbName))

    fun start() {
        mIsRunning = true
        Thread(this).start()
    }

    fun stop() {
        try {
            mIsRunning = false
            val serverSocket = mServerSocket ?: return
            serverSocket.close()
            mServerSocket = null
        }catch (e:java.lang.Exception) {

        }
    }

    override fun run() {
        try {
            mServerSocket = ServerSocket(port)
            while (mIsRunning) {
                val socket = mServerSocket?.accept()
                if (socket != null) {
                    request.handle(socket)
                    socket.close()
                }
            }
        } catch (e: SocketException) {
            e.printStackTrace()
        }
    }
}