package paperdb.io.paperdb

import android.content.Context
import android.preference.PreferenceManager
import paperdb.io.paperdb.db.CarDBHelper
import paperdb.io.paperdb.db.ContactDBHelper
import paperdb.io.paperdb.db.ExtTestDBHelper
import paperdb.io.paperdb.db.rom.User
import paperdb.io.paperdb.db.rom.UserDBHelper

/**
 * @author lazyduck037
 */
object WireDb {
    fun write(app:Context){
        val stringSet: MutableSet<String> = HashSet()
        stringSet.add("SetOne")
        stringSet.add("SetTwo")
        stringSet.add("SetThree")

        val sharedPreferences = PreferenceManager.getDefaultSharedPreferences(app);

        val prefsOne = app.getSharedPreferences("countPrefOne", Context.MODE_PRIVATE);
        val prefsTwo = app.getSharedPreferences("countPrefTwo", Context.MODE_PRIVATE);

        sharedPreferences.edit().putString("testOne", "one").commit();
        sharedPreferences.edit().putInt("testTwo", 2).commit();
        sharedPreferences.edit().putLong("testThree", 100000L).commit();
        sharedPreferences.edit().putFloat("testFour", 3.01F).commit();
        sharedPreferences.edit().putBoolean("testFive", true).commit();
        sharedPreferences.edit().putStringSet("testSix", stringSet).commit();

        prefsOne.edit().putString("testOneNew", "one").commit();

        prefsTwo.edit().putString("testTwoNew", "two").commit();

        val contactDBHelper = ContactDBHelper(app);
        if (contactDBHelper.count() == 0) {
            for (i in 0..99) {
                val name = "name_$i";
                val phone = "phone_$i";
                val email = "email_$i";
                val street = "street_$i";
                val place = "place_$i";
                contactDBHelper.insertContact(name, phone, email, street, place);
            }
        }

        val carDBHelper =  CarDBHelper(app)
        if (carDBHelper.count() == 0) {
            for (i in 0..49) {
                val name = "name_$i"
                val color = "RED"
                val mileage = i + 10.45f
                carDBHelper.insertCar(name, color, mileage)
            }
        }

        val extTestDBHelper = ExtTestDBHelper(app);
        if (extTestDBHelper.count() == 0) {
            for (i in 0..19) {
                val value = "value_$i";
                extTestDBHelper.insertTest(value);
            }
        }

        // Room database
        val userDBHelper = UserDBHelper(app);
        if (userDBHelper.count() == 0) {
           val userList = ArrayList<User>()
            for (i in 0..19) {
                val user = User();
                user.id = (i + 1).toLong()
                user.name = "user_" + i;
                userList.add(user);
            }
            userDBHelper.insertUser(userList);
        }
    }
}