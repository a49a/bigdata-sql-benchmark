package com.github.a49a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

public class HdfsBatchRename {
    public static void main(String[] args) throws IOException {
        String tableName = "catalog_sales";
        String path = "hdfs://ns1/dtInsight/hive/warehouse/tpcds_bin_partitioned_parquet_3.db/" + tableName;
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/ada/opt/hadoop-2.8.5/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/ada/opt/hadoop-2.8.5/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Arrays.stream(fs.listStatus(new Path(path))).forEach(fileStatus -> {
            try {
                Arrays.stream(fs.listStatus(fileStatus.getPath())).forEach(partFileStatus -> {
                    System.out.println(partFileStatus.getPath().toString());
                    try {
                        fs.rename(new Path(partFileStatus.getPath().toString()), new Path(partFileStatus.getPath().toString() + ".parquet"));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(partFileStatus.getPath().toString() + ".parquet");
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
