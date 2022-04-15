package com.github.deadwind4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import static com.github.deadwind4.QueryUtil.getQueries;

public class Benchmark {

    private static final Option HIVE_CONF = new Option("c", "hive_conf", true,
            "conf of hive.");

    private static final Option DATABASE = new Option("d", "database", true,
            "database of hive.");

    private static final Option LOCATION = new Option("l", "location", true,
            "sql query path.");

    private static final Option QUERIES = new Option("q", "queries", true,
            "sql query names. If the value is 'all', all queries will be executed.");

    private static final Option ITERATIONS = new Option("i", "iterations", true,
            "The number of iterations that will be run per case, default is 1.");

    private static final Option PARALLELISM = new Option("p", "parallelism", true,
            "The parallelism, default is 800.");

    public static void main(String[] args) throws ParseException {
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        run(
                getQueries(
                        line.getOptionValue(LOCATION.getOpt()),
                        line.getOptionValue(QUERIES.getOpt())),
                Integer.parseInt(line.getOptionValue(ITERATIONS.getOpt(), "1"))
        );
    }

    private static Connection getConnection() {
        // URL parameters
//                    "jdbc:trino://host:port/catalog/schema"
        String url = "jdbc:trino://172.16.85.185:8080/hive3/tpcds_bin_orc_10";
        String uri = "jdbc:trino://trino_1:8080/hive3/tpcds_bin_orc_10";
//        properties.setProperty("user", "test");
//        properties.setProperty("password", "secret");
//        properties.setProperty("SSL", "true");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, "admin", null);
        } catch (SQLException e) {
            System.out.println(e);
            System.out.println(e.getMessage());
        }
        return conn;
    }

    private static void run(LinkedHashMap<String, String> queries, int iterations) {

        try (Connection conn = getConnection()) {
//            conn.createStatement().executeQuery("USE hive3.tpcds_bin_orc_10");
            List<Tuple2<String, Long>> bestArray = new ArrayList<>();
            queries.forEach((name, sql) -> {
                System.out.println("Start run query: " + name);
                Runner runner = new Runner(name, sql, iterations, conn);
                runner.run(bestArray);
            });
            printSummary(bestArray);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void printSummary(List<Tuple2<String, Long>> bestArray) {
        if (bestArray.isEmpty()) {
            return;
        }
        System.err.println("--------------- tpcds Results ---------------");
        int itemMaxLength = 20;
        System.err.println();
        long total = 0L;
        double product = 1d;
        printLine('-', "+", itemMaxLength, "", "");
        printLine(' ', "|", itemMaxLength, " " + "tpcds sql", " Time(ms)");
        printLine('-', "+", itemMaxLength, "", "");

        for (Tuple2<String, Long> tuple2 : bestArray) {
            printLine(' ', "|", itemMaxLength, tuple2.f0, String.valueOf(tuple2.f1));
            total += tuple2.f1;
            product = product * tuple2.f1 / 1000d;
        }

        printLine(' ', "|", itemMaxLength, "Total", String.valueOf(total));
        printLine(' ', "|", itemMaxLength, "Average", String.valueOf(total / bestArray.size()));
        printLine(' ', "|", itemMaxLength, "GeoMean", String.valueOf((java.lang.Math.pow(product, 1d / bestArray.size()) * 1000)));
        printLine('-', "+", itemMaxLength, "", "");

        System.err.println();
    }

    static void printLine(
            char charToFill,
            String separator,
            int itemMaxLength,
            String... items) {
        StringBuilder builder = new StringBuilder();
        for (String item : items) {
            builder.append(separator);
            builder.append(item);
            int left = itemMaxLength - item.length() - separator.length();
            for (int i = 0; i < left; i++) {
                builder.append(charToFill);
            }
        }
        builder.append(separator);
        System.err.println(builder.toString());
    }


    private static Options getOptions() {
        Options options = new Options();
        options.addOption(HIVE_CONF);
        options.addOption(DATABASE);
        options.addOption(LOCATION);
        options.addOption(QUERIES);
        options.addOption(ITERATIONS);
        options.addOption(PARALLELISM);
        return options;
    }
}
