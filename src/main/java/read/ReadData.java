package read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;


public class ReadData {
    static Logger logger = Logger.getLogger("MyLog");
        public  Dataset<Row> read(String path ,SparkSession sparkSession ) {
            logger.info("read method call..");
        return sparkSession.read().option("header", "true").csv(path);

    }



}
