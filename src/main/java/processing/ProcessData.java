package processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

public class ProcessData {

    static Logger logger = Logger.getLogger("MyLog");
    public  Dataset<Row> getTableData(SparkSession sprkSecc , String sqlQuery) {
        logger.info("getTableData method call");

        return sprkSecc.sql(sqlQuery);

    }

}
