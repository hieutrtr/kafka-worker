package com.hieuslash;
import java.lang.annotation.Annotation;
import java.util.Properties;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.hieuslash.workers.Worker;

public class WorkerConfig {

  public static Properties getConsumerConfig(Class workerCls) {
    String configPath = null;
    String consumerPropsPath = null;
    Properties props = new Properties();

    if(workerCls.isAnnotationPresent(WorkerBoot.class))
    {
      Annotation anno = workerCls.getAnnotation(WorkerBoot.class);
      WorkerBoot workerBoot = (WorkerBoot) anno;
      configPath = workerBoot.configPath();
      consumerPropsPath = workerBoot.consumerPropsPath();
    }

    try {
      props = loadPropertyFile(configPath,consumerPropsPath);
    } catch (Exception e) {
      System.out.println(configPath + "/" + consumerPropsPath);
    } finally {
      return props;
    }

  }

  private static Properties loadPropertyFile(String configPath,String propertiesPath) throws IOException {
      Properties props = new Properties();
      File temp = new File(configPath , propertiesPath);
      InputStream inputStream = new FileInputStream(temp);
      try{
        if(inputStream != null)
        {
          props.load(inputStream);
        }
        else{
          throw new FileNotFoundException("Property file '" + configPath + "/" + propertiesPath + "' not found in the path");
        }
      } catch (Exception e) {
        System.out.println("Exception: " + e);
      } finally {
        inputStream.close();
      }
      return props;
  }
}
