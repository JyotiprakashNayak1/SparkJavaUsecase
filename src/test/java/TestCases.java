import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import java.util.logging.Logger;


public class TestCases {

    public static final String HEADERS = "header";
    static Logger logger = Logger.getLogger("MyLog");
    public static SparkSession sprkSecc = SparkSession.builder().appName("uses cases").master("local").getOrCreate();
    ;
    public static final String F_STRING ="Test case";
    public static final String L_STRING ="is success";
    private Dataset<Row> readfromdesire;
    private Dataset<Row> readfromdesiretest;
    private Dataset<Row> dormateCustread;
    private Dataset<Row> dormateCustreadtest;
    private Dataset<Row> revPerCust;
    private Dataset<Row> revPerCustTest;
    private Dataset<Row> revPerCat;
    private Dataset<Row> revPerCatTest;
    private  Dataset<Row> prodCountPerDept;
    private Dataset<Row> prodCountPerDeptTest;
    public static void main(String[] args) throws AnalysisException {

        String customer = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\customers\\part*";
        String catagory = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\categories\\part*";
        String department = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\departments\\part*";
        String orderitems = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\order_items\\part*";
        String orders = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\orders\\part*";
        String product = "C:\\Users\\Jyotiprakash Nayak\\Mini\\retail_db\\retail_db\\products\\part*";

        Dataset<Row> dCustomer = sprkSecc.read().option(HEADERS, "true").csv(customer);
        Dataset<Row> dfCatagory = sprkSecc.read().option(HEADERS, "true").csv(catagory);
        Dataset<Row> dfDepartment = sprkSecc.read().option(HEADERS, "true").csv(department);
        Dataset<Row> dfOrderItems = sprkSecc.read().option(HEADERS, "true").csv(orderitems);
        Dataset<Row> dfOrder = sprkSecc.read().option(HEADERS, "true").csv(orders);
        Dataset<Row> dfProduct = sprkSecc.read().option(HEADERS, "true").csv(product);

        dCustomer.createTempView("customers");
        dfOrder.createTempView("orders");
        dfOrderItems.createTempView("order_items");
        dfCatagory.createTempView("categories");
        dfDepartment.createTempView("departments");
        dfProduct.createTempView("products");

        Dataset<Row> readfromdesire =sprkSecc.read().option("header", "true").csv("C:\\Users\\Public\\Documents\\project\\retail_db\\co_count");
        Dataset<Row> readfromdesiretest= sprkSecc.sql("select departments.*,count(*),"  +
                "count(product_id) as product_count " +
                "from departments JOIN categories " +
                "on departments.department_id = categories.category_department_id " +
                "JOIN products on products.product_category_id= categories.category_id " +
                "GROUP BY department_id,department_name");
        Dataset<Row> dormateCustread = sprkSecc.read().option("header", "true").csv("C:\\Users\\Public\\Documents\\project\\retail_db\\dormant_count");
        Dataset<Row> dormateCustreadtest =sprkSecc.sql("select ca.*, " +
                "round(sum(oi.order_item_subtotal), 2) AS customer_revenue " +
                "                                    FROM orders o " +
                "                                       JOIN order_items oi " +
                "                                      ON o.order_id = oi.order_item_order_id " +
                "                                    JOIN products on products.product_id=oi.order_item_product_id " +
                "                                      JOIN categories ca on category_id= product_category_id " +
                "                                       GROUP BY ca.category_id, " +
                "                                           ca.category_department_id, " +
                "                                           ca.category_name " +
                "                                       ORDER BY " +
                "                                      category_id");

        Dataset<Row> revPerCust = sprkSecc.read().option("header", "true").csv("C:\\Users\\Public\\Documents\\project\\retail_db\\rev_per_cat");
        Dataset<Row> revPerCustTest =sprkSecc.sql("select customer_id, " +
                "                          customer_fname as customer_first_name, " +
                "                          customer_lname as customer_last_name, " +
                "                          round(sum(oi.order_item_subtotal), 2) AS customer_revenue "+
                "                          From orders as o " +
                "                          join customers as c "+
                "                          on o.order_customer_id=c.customer_id "+
                "                          join order_items as oi "+
                "                          on o.order_id = oi.order_item_id "+
                "                          GROUP BY customer_id, " +
                "                              customer_first_name, " +
                "                              customer_last_name "+
                "                          ORDER BY customer_revenue desc, " +
                "                          customer_id");
        Dataset<Row> revPerCat = sprkSecc.read().option("header", "true").csv("C:\\Users\\Public\\Documents\\project\\retail_db\\rev_per_cus");
        Dataset<Row> revPerCatTest = sprkSecc.sql("select customers.* "+
                "                                From orders  join customers " +
                "                                on orders.order_customer_id=customers.customer_id " +
                "                                where order_date LIKE '2014-01%' " +
                "                                AND order_status IS NULL");
        Dataset<Row> prodCountPerDept = sprkSecc.read().option("header", "true").csv("C:\\Users\\Public\\Documents\\project\\retail_db\\rev_per_dep");
        Dataset<Row> prodCountPerDeptTest =sprkSecc.sql("Select customer_id, " +
                "                                        customer_fname as customer_first_name, " +
                "                                        customer_lname as customer_last_name, " +
                "                                        count(order_status) As customers_order_count " +
                "                                        From orders  join customers " +
                "                                        on orders.order_customer_id=customers.customer_id " +
                "                                        where order_date LIKE '2014-01%' " +
                "                                        GROUP BY customer_id, " +
                "                                        customer_first_name, " +
                "                                        customer_last_name " +
                "                                        ORDER BY " +
                "                                        customers_order_count desc, " +
                "                                        customer_id " +
                "                                        Limit 5");
        }
        @Test
        public void testUseCaseOne(){
        System.out.println(readfromdesire);
        System.out.println(readfromdesiretest);
        assertEquals("This method is for testing customer order count",readfromdesire,readfromdesiretest);


    }

    @Test
    public void testUseCaseTwo(){
        System.out.println(dormateCustread);
        System.out.println(dormateCustreadtest);
        assertEquals("This method is for testing dormant customer",dormateCustread,dormateCustreadtest);


    }

    @Test
    public void testUseCaseThree(){
        System.out.println(revPerCust);
        System.out.println(revPerCustTest);
        assertEquals("This method is for testing revenue per customer",revPerCust,revPerCustTest);


    }

    @Test
    public void testUseCaseFour(){
        System.out.println(revPerCat);
        System.out.println(revPerCatTest);
        assertEquals("This method is for testing revenue per category",revPerCat,revPerCatTest);


    }

    @Test
    public void testUseCaseFive(){
        System.out.println(prodCountPerDept);
        System.out.println(prodCountPerDeptTest);
        assertEquals("This method is for testing product count per department",prodCountPerDept,prodCountPerDeptTest);

        logger.info(F_STRING+"five"+L_STRING);
    }

    }

