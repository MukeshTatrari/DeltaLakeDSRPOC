package com.example.DeltaLake;

import io.delta.standalone.DeltaLog;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sessionBuilder() {
        SparkSession spark = SparkSession.builder().appName("Spark Excel file conversion")
                .config("spark.master", "local")
                .config("spark.sql.codegen.wholeStage", false)
                .config("fs.defaultFS", Constants.ABSS_FILE_PATH)
                .config("fs.azure.account.key.ubsgen2storageuk.dfs.core.windows.net", Constants.ACCESS_KEY)
                .getOrCreate();
        return spark;
    }

    @Bean
    public DeltaLog deltaLogBuilder() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", Constants.ABSS_FILE_PATH);
        conf.set("fs.azure.account.key.ubsgen2storageuk.dfs.core.windows.net", Constants.ACCESS_KEY);
        DeltaLog log = DeltaLog.forTable(conf, new Path(Constants.empTableName));
        return log;
    }
}
