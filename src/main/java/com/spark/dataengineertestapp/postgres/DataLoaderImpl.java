
package com.spark.dataengineertestapp.postgres;


import com.spark.dataengineertestapp.Application;
import com.spark.dataengineertestapp.ConfigManager;

import java.nio.charset.Charset;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import java.sql.DriverManager;
import java.sql.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.List;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import javax.json.*;
import com.spark.dataengineertestapp.kafka.*;

public class DataLoaderImpl implements IDataLoader {

  private ConfigManager config = null;
  private String schema_name = null;
  private Connection conn = null;
  private static final Logger logger = LoggerFactory.getLogger (DataLoaderImpl.class);
  private SparkSession spark =null;
  private IProducer kafkaProd = null;
  
  public DataLoaderImpl (ConfigManager config,IProducer kafkaProd) {
    this.config = config;
    schema_name = config.getProperty ("schema_name");
    this.kafkaProd = kafkaProd;
    spark = SparkSession
     .builder()
     .appName("spark data engineer test")
     .config(new SparkConf().setAppName("MyTaxiApp").setMaster("local[2]").set("spark.executor.memory","1g"))
     .getOrCreate();
    try{
      Class.forName ("org.postgresql.Driver");     
    }
    catch(Exception ex)
    {
      ex.printStackTrace();
    }
  }

  protected FileInputStream getFileInputStream (String filePath) throws FileNotFoundException {

    FileInputStream fileInputStream;
    fileInputStream = new FileInputStream (filePath);
    return fileInputStream;
  }

  public String getTableDDL (String ddlPath) {

    BufferedReader br = null;
    FileReader fr = null;
    StringBuffer buf = new StringBuffer ();
    try {
      fr = new FileReader (ddlPath);
      br = new BufferedReader (fr);

      String sCurrentLine = null;
      while ((sCurrentLine = br.readLine ()) != null) {
        buf.append (sCurrentLine);
      }

    } catch (Exception ex) {
      ex.printStackTrace ();
    }
    return buf.toString ();
  }

  public void createTable (String connectionUrl, String tableName, String ddlFilePath) {

    try {
      Connection con = DriverManager.getConnection (connectionUrl);
      Statement st = con.createStatement ();

      String driverddl = getTableDDL (ddlFilePath);
      try {
        st.executeUpdate (driverddl);
      } catch (Exception ex) {
        logger.info ("ignoring error if table exists " + ex);
      }
    } catch (Exception ex) {
       ex.printStackTrace ();
    }
  }

  @Override
  public void toDb ( String connectionUrl, String tableName, String filePath) {

    try {
      Connection con = DriverManager.getConnection (connectionUrl);
      String copyStatement =
          "COPY " + schema_name + "." + tableName + " FROM STDIN WITH DELIMITER ','";
      FileInputStream fileInputStream = getFileInputStream (filePath);
      InputStreamReader inputStreamReader = getInputStreamReader (fileInputStream);
      CopyManager copyManager = new CopyManager (con.unwrap (BaseConnection.class));
      copyManager.copyIn (copyStatement, inputStreamReader);

    } catch (Exception ex) {
      ex.printStackTrace ();
    }

  }

  protected InputStreamReader getInputStreamReader (FileInputStream fileInputStream) {

    InputStreamReader inputStreamReader;
    inputStreamReader = new InputStreamReader (fileInputStream, Charset.defaultCharset ());
    return inputStreamReader;
  }

  public String getJDBCURL () {

    String hostname = config.getProperty ("postgresql_db_host");
    String port = config.getProperty ("postgresql_db_port");
    String db_name = config.getProperty ("postgresql_db_name");
    String db_user = config.getProperty ("postgresql_db_user");
    String db_pass = config.getProperty ("postgresql_db_pass");

    String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + db_name + "?user="
        + db_user + "&password=" + db_pass;
    return jdbcUrl;
  }
  
  public void topTenDrivers(String connUrl) {
    
    Dataset<Row> driverInfo = spark.read()
        .format("jdbc")
        .option("url",connUrl )
        .option("dbtable", schema_name+"."+Application.driver)
        .load();
    Dataset<Row> bookings = spark.read ()
        .format("jdbc")
        .option("url",connUrl)
        .option("dbtable",schema_name+"."+Application.booking)
        .load();
    Dataset<Row> res = driverInfo.join (bookings,driverInfo.col ("id").equalTo (bookings.col ("id_driver")));
    Dataset<Row> res1 =  res.groupBy (res.col ("name")).avg("rating");
    Dataset<Row> res2 = res.groupBy (res.col("name")).sum ("tour_value");
    Dataset<Row> res3 = res2.groupBy(res2.col ("name")).max("sum(tour_value)");
    Dataset<Row> res4 = res1.join (res3,res1.col ("name").equalTo(res3.col("name")));
    Dataset<Row> res5 = res4.orderBy (desc("max(sum(tour_value))"),desc("avg(rating)")).limit(10);
    
   
    List<Row> res6 = res5.collectAsList ();
    logger.info ("=====================================================");
    logger.info ("           Top 10 drivers                        ");
    logger.info ("name         |   rating                 | tour_value");
    JsonObjectBuilder listofDrivers = Json.createObjectBuilder();
    JsonArrayBuilder arrayDriver =   Json.createArrayBuilder();
    listofDrivers.add("drivers",arrayDriver);
             
    for( Row r: res6 ) {
      logger.info (r.get (0)+"|        "+r.get(1)+"|                  "+r.get(3));
      JsonObjectBuilder arow = Json.createObjectBuilder();
      arow.add("name",(String)r.get(0));
      arow.add("rating",(Double)r.get (1));
      arow.add("tour_value",(Long)r.get (3));
      arrayDriver.add(arow);
    }
    logger.info ("====================================================");
    kafkaProd.send(config.getProperty ("top10drivertopic"),listofDrivers.build().toString());
    
  }
  
  public void passengerDriverFavorite(String connUrl) {
    
    Dataset<Row> passenger = spark.read()
        .format("jdbc")
        .option("url",connUrl )
        .option("dbtable", schema_name+"."+Application.passenger)
        .load();
    Dataset<Row> bookings = spark.read ()
        .format("jdbc")
        .option("url",connUrl)
        .option("dbtable",schema_name+"."+Application.booking)
        .load();
    Dataset<Row> driverInfo = spark.read()
        .format("jdbc")
        .option("url",connUrl )
        .option("dbtable", schema_name+"."+Application.driver)
        .load();
    
    Dataset<Row> res1 = bookings.groupBy(bookings.col("id_passenger"),bookings.col("id_driver")).count();
    Dataset<Row> res2 = res1.orderBy(desc("count")).limit (10);
    Dataset<Row> res3 = res2.join (driverInfo,res2.col("id_driver").equalTo (driverInfo.col("id")));
    Dataset<Row> res4 = res3.join (passenger,res3.col("id_passenger").equalTo(passenger.col ("id")));
    List<Row> res5 = res4.collectAsList ();
    JsonObjectBuilder listPassToDrivers = Json.createObjectBuilder();
    JsonArrayBuilder arrayPD =   Json.createArrayBuilder();
    listPassToDrivers.add("passengertodriver",arrayPD);
    
    logger.info ("========================================================================");
    logger.info ("           Passenger's Favorite Driver               ");
    logger.info ("Passenger's Name         |   Driver's Name                 | No of Trips");
    
    for( Row r: res5 ) {
      logger.info (r.get(8)+"|                 "+r.get (5)+"|                    "+r.get (2));
      JsonObjectBuilder arow = Json.createObjectBuilder();
      arow.add("passenger_name",(String)r.get(8));
      arow.add("driver_name",(String)r.get (5));
      arow.add("no_of_trips",(Long)r.get (2));
      arrayPD.add(arow);
    }
    logger.info ("========================================================================");
    kafkaProd.send(config.getProperty ("passengerfavtopic"),listPassToDrivers.build().toString());
    
  }
  
  public void yearlyKpi(String connUrl) {
    
    Dataset<Row> bookings = spark.read ()
        .format("jdbc")
        .option("url",connUrl)
        .option("dbtable",schema_name+"."+Application.booking)
        .load();
    
    Dataset<Row> res1 = bookings.agg (sum("tour_value"));
    Dataset<Row> res2 = bookings.agg (avg("rating"));
    List<Row> revenue = res1.collectAsList();
    List<Row> rating = res2.collectAsList ();
    logger.info ("========================================================");
    logger.info ("                  Yearly KPI                            ");
    logger.info ("        Total Revenue        |   Average Driver Rating  ");
    Long totalRevenue = revenue.get (0).getLong(0);
    Double avgRating = rating.get (0).getDouble (0);
    logger.info (totalRevenue+"|                           "+avgRating);
    logger.info ("========================================================");
    JsonObjectBuilder yearlyKPI = Json.createObjectBuilder();
    JsonObjectBuilder arow = Json.createObjectBuilder();
    arow.add("revenue",totalRevenue);
    arow.add("avg_rating",avgRating);
    yearlyKPI.add("yearly_kpi",arow);
    kafkaProd.send(config.getProperty ("yearlykpi"),yearlyKPI.build().toString());
    
  }

}
