package com.spark.dataengineertestapp;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;



public class ConfigManager {
  
  private static final String configFile = "src/main/java/resources/config.properties";

  private InputStream input = null;
  private Properties prop=null;
  public ConfigManager() {
    try {
      input = new FileInputStream(configFile);
      prop  = new Properties();
      prop.load (input);
    }
    catch (IOException ex)
    {
      ex.printStackTrace ();
    }
    finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  public String getProperty(String name)
  {
    return prop.getProperty(name);
  }
}
