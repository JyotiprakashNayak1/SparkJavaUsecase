package writedata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

public class WriteData {
    static Logger logger = Logger.getLogger("MyLog");
    public void writeData(Dataset<Row> writeLocation, String path){
        writeLocation.coalesce(1).write().option("header","true").mode("overwrite").csv(path);
        logger.info("writing data to the location " );

    }
}


