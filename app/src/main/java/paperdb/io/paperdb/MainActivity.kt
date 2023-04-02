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
                Paper.book().write("product", Product("12L","Hello", "new product"))
                Paper.book().write("primary", 1)
                Paper.book().write("string", "1")
                Paper.book().write("boolean", true)
                Paper.book().write("arrayPrimary", arrayListOf(1,2,3, null))
                Paper.book().write("arrayString", arrayListOf("1","2","3", null))
                Paper.book().write("arrayObject",
                    listOf( Product("12L","Hello", "new product"),
                        Product("12L","Hello", "new product"),
                        Product("12L","Hello", "new product"),
                        Product("12L","Hello", "new product")))
                Paper.book().write("hashmapPrimary", hashMapOf("1" to 1,"2" to 2,"3" to 3, "4" to null))
                Paper.book().write("hashmapString", hashMapOf("1" to "1","2" to "2","3" to "3", "4" to null))
                Paper.book().write("hashmapObject", hashMapOf(
                    "1" to Product("12L","Hello", "new product"),
                    "2" to Product("12L","Hello", "new product"),
                    "3" to null)
                )

                val array = ArrayList<Product>()
                for (i in 0..50){
                    array.add(Product("$i","Hello", "new product"))
                }
                Paper.book().write("paging",array)
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