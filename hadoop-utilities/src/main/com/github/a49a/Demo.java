package com.github.a49a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

public class Demo {
    public static void main(String[] args) throws IOException {
        String path = "hdfs://ns1/tmp/foo_table/";
        Configuration conf = new Configuration();
        conf.addResource(new Path("/Users/ada/opt/hadoop-2.8.5/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/Users/ada/opt/hadoop-2.8.5/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Arrays.stream(fs.listStatus(new Path(path))).forEach(fileStatus -> {
            try {
                fs.rename(new Path(fileStatus.getPath().toString() + "/foo_f"), new Path(fileStatus.getPath().toString() + "/bar_f"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
