package mainclass;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import processing.ProcessData;
import read.ReadData;
import writedata.WriteData;

import java.util.logging.Logger;

public class Starting {
   private static final Logger logger = Logger.getLogger("MyLog");
    private static final String CUSTOMER = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\customers\\part*";
    private static final String CATAGORY = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\categories\\part*";
    private static final String DEPARTMENT = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\departments\\part*";
    private static final String ORDERITEMS = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\order_items\\part*";
    private static final String ORDERS = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\orders\\part*";
    private static final String PRODUCT = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\products\\part*";

    private static final String WRITE_PATH = "wrtpath";

    private static final String CUSTOMER_ORDER_COUNT = "select departments.*,count(product_id) as product_count from departments JOIN categories on departments.department_id = categories.category_department_id JOIN products on products.product_category_id= categories.category_id GROUP BY department_id,department_name";
    private static final String DORMANT_CUSTOMER =  "select ca.*, " +
                                                     "round(sum(oi.order_item_subtotal), 2) AS customer_revenue " +
                                                     "FROM orders o " +
                                                     "JOIN order_items oi " +
                                                     "ON o.order_id = oi.order_item_order_id " +
                                                     "JOIN products on products.product_id=oi.order_item_product_id " +
                                                     "JOIN categories ca on category_id= product_category_id " +
                                                     "GROUP BY ca.category_id, " +
                                                     "ca.category_department_id, " +
                                                     "ca.category_name " +
                                                     "ORDER BY " +
                                                     "category_id";
    private static final String REVENUE_PER_CUSTOMER = "select customer_id, " +
                                                           "customer_fname as customer_first_name, " +
                                                           "customer_lname as customer_last_name, " +
                                                           "round(sum(oi.order_item_subtotal), 2) AS customer_revenue "+
                                                           "From orders as o " +
                                                           "join customers as c "+
                                                           "on o.order_customer_id=c.customer_id "+
                                                           "join order_items as oi "+
                                                           "on o.order_id = oi.order_item_id "+
                                                           "GROUP BY customer_id, " +
                                                           "customer_first_name, " +
                                                           "customer_last_name "+
                                                           "ORDER BY customer_revenue desc, " +
                                                           "customer_id";
    private static final String REVENUE_PER_CATEGORY = "select customers.* "+
                                                           "From orders  join customers " +
                                                           "on orders.order_customer_id=customers.customer_id " +
                                                           "where order_date LIKE '2014-01%' " +
                                                           "AND order_status IS NULL";
    private static final String PRODUCT_COUNT_PER_DEPARTMENT = "Select customer_id, " +
                                                                                   "customer_fname as customer_first_name, " +
                                                                                   "customer_lname as customer_last_name, " +
                                                                                   "count(order_status) As customers_order_count " +
                                                                                   "From orders  join customers " +
                                                                                   "on orders.order_customer_id=customers.customer_id " +
                                                                                   "where order_date LIKE '2014-01%' " +
                                                                                   "GROUP BY customer_id, " +
                                                                                   "customer_first_name, " +
                                                                                   "customer_last_name " +
                                                                                   "ORDER BY " +
                                                                                   "customers_order_count desc, " +
                                                                                   "customer_id " +
                                                                                   "Limit 5";

    public static void main(String[] args ) throws AnalysisException
    {

        logger.info("Application Started..");
        ReadData readData = new ReadData();
        Dataset<Row> dCustomer = readData.read(CUSTOMER,getSession());
        Dataset<Row> dfCatagory =readData. read(CATAGORY,getSession());
        Dataset<Row> dfDepartment = readData.read(DEPARTMENT,getSession());
        Dataset<Row> dfOrderItems = readData.read(ORDERITEMS,getSession());
        Dataset<Row> dfOrder = readData.read(ORDERS,getSession());
        Dataset<Row> dfProduct = readData.read(PRODUCT,getSession());

        dCustomer.createTempView("customers");
        dfOrder.createTempView("orders");
        dfOrderItems.createTempView("order_items");
        dfCatagory.createTempView("categories");
        dfDepartment.createTempView("departments");
        dfProduct.createTempView("products");

        ProcessData processData = new ProcessData();
        WriteData writeData = new WriteData();

        Dataset<Row> dfPCPerDepartment = processData.getTableData(getSession(),CUSTOMER_ORDER_COUNT);
        writeData.writeData(dfPCPerDepartment,WRITE_PATH+"co_count");

        Dataset<Row> dfRPCatagory = processData.getTableData(getSession(),DORMANT_CUSTOMER);
        writeData.writeData(dfRPCatagory,WRITE_PATH+"dormant_count");

        Dataset<Row> dfRPCustomer = processData.getTableData(getSession(),REVENUE_PER_CUSTOMER);
        writeData.writeData(dfRPCustomer,WRITE_PATH+"rev_per_cus");

               Dataset<Row> dfDMCustomer = processData.getTableData(getSession(),REVENUE_PER_CATEGORY);
        writeData.writeData(dfDMCustomer,WRITE_PATH+"rev_per_cat");

               Dataset<Row> dfOrderCount = processData.getTableData(getSession(),PRODUCT_COUNT_PER_DEPARTMENT);
        writeData.writeData(dfOrderCount,WRITE_PATH+"rev_per_dep");
           }

    public static SparkSession getSession()
    {
        logger.info(":: SparkSession created.. ");
        return SparkSession.builder().appName("uses cases").master("local").getOrCreate();
    }

}
