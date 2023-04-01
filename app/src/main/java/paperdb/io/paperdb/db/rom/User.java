package paperdb.io.paperdb.db.rom;

import androidx.room.Entity;
import androidx.room.PrimaryKey;

/**
 * Created by anandgaurav on 12/02/18.
 */
@Entity(tableName = "users")
public class User {

    @PrimaryKey
    public Long id;

    public String name;

}
