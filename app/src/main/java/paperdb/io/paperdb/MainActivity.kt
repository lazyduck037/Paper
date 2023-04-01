package paperdb.io.paperdb

import android.os.Bundle
import android.util.Log
import android.view.View
import androidx.appcompat.app.AppCompatActivity
import io.db.inspector.InspectorDb
import io.paperdb.Paper

class MainActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        Paper.init(this)

        findViewById<View>(R.id.test_write).setOnClickListener {
            Thread{
                Paper.book().write("o2", Product("12L","Hello", "new product"))
                Paper.book().write("o3", Product("12L","Hello", "new product"))
                Paper.book().write("o4", Product("12L","Hello", "new product"))
                WireDb.write(this.application)
            }.start()
        }

        findViewById<View>(R.id.test_read).setOnClickListener {
            Thread{
                val res = Paper.book().read<Any>("o2")
                Log.d("res","$res")
            }.start()
        }

    }

}