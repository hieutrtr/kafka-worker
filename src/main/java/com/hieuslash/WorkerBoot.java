package com.hieuslash;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WorkerBoot {
  String configPath() default "/etc/kafka-worker";
  String consumerPropsPath() default "config/consumer.properties";
  String esPropsPath() default "config/es.properties";
}
