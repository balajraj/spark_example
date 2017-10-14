package com.spark.dataengineertestapp;


import com.spark.dataengineertestapp.postgres.*;
import com.spark.dataengineertestapp.kafka.*;
public class Application
{
    
    private ConfigManager config = null;
    public static final String driver="driver";
    public static final String passenger ="passenger";
    public static final String booking="booking";
    private DataLoaderImpl dataload=null;
    private String connUrl = null;
    private IProducer kafkaprod = null;
    public Application (ConfigManager config)
    {
      this.config = config;
      
      kafkaprod = new ProducerImpl(config.getProperty ("bootstrap_server")); 
      dataload = new DataLoaderImpl(config,kafkaprod);
      connUrl =  dataload.getJDBCURL();
    }
    
    public void loadData() {
      
      dataload.createTable (connUrl, driver, config.getProperty ("driver_ddl"));
      dataload.toDb(connUrl,driver,config.getProperty("driver_data_file"));      
      dataload.createTable (connUrl, passenger, config.getProperty ("passenger_ddl"));
      dataload.toDb(connUrl,passenger,config.getProperty("passenger_data_file"));
      dataload.createTable (connUrl, booking, config.getProperty ("booking_ddl"));
      dataload.toDb(connUrl,booking,config.getProperty("booking_data_file"));
    
    }
    
    public void top10Drivers()
    {
      dataload.topTenDrivers (connUrl);
    }
    
    public void passengerDriverFav() {
      dataload.passengerDriverFavorite (connUrl);
    }
    
    public void yearlyKPI() {
      dataload.yearlyKpi (connUrl);
    }
    
    public static void main(String[] args)
    {
       Application app = new Application(new ConfigManager());
       app.loadData ();
       app.top10Drivers();
       app.passengerDriverFav ();
       app.yearlyKPI();
    }
}
