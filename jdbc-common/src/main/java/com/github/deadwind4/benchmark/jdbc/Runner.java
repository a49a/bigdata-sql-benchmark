package com.github.deadwind4.benchmark.jdbc;

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

class Runner {

    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

    private final String name;
    private final String sqlQuery;
    private final int numIters;
    private final Connection conn;

    Runner(String name, String sqlQuery, int numIters, Connection conn) {
        this.name = name;
        this.sqlQuery = sqlQuery;
        this.numIters = numIters;
        Preconditions.checkArgument(numIters > 0);
        this.conn = conn;
    }

    void run(List<Tuple2<String, Long>> bestArray) {
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < numIters; ++i) {
            System.err.println(
                    String.format("--------------- Running %s %s/%s ---------------", name, (i + 1), numIters));
            try {
                results.add(runInternal());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        printResults(results, bestArray);
    }

    private Result runInternal() throws Exception {
        // ensures garbage from previous cases don't impact this one
        System.gc();

        long startTime = System.currentTimeMillis();

        LOG.info(" begin execute.");
        ResultSet rs = conn.createStatement().executeQuery(sqlQuery);
        LOG.info(" end execute");
        System.out.println();

//        long totalTime = System.currentTimeMillis() - startTime;
//        System.out.println("total execute " + totalTime + "ms.");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("total execute " + totalTime + "ms.");

        return new Result(totalTime);
    }

    private void printResults(List<Result> results, List<Tuple2<String, Long>> bestArray) {
        int itemMaxLength = 20;
        System.err.println();
        Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");
        Benchmark.printLine(' ', "|", itemMaxLength, " " + name, " Best Time(ms)", " Avg Time(ms)", " Max Time(ms)");
        Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");

        Tuple3<Long, Long, Long> t3 = getBestAvgMaxTime(results);
        Benchmark.printLine(' ', "|", itemMaxLength, " Total", " " + t3.f0, " " + t3.f1, " " + t3.f2);
        Benchmark.printLine('-', "+", itemMaxLength, "", "", "", "");
        bestArray.add(new Tuple2<>(name, t3.f0));
        System.err.println();
    }

    private Tuple3<Long, Long, Long> getBestAvgMaxTime(List<Result> results) {
        long best = Long.MAX_VALUE;
        long sum = 0L;
        long max = Long.MIN_VALUE;
        for (Result result : results) {
            long time = result.totalTime;
            if (time < best) {
                best = time;
            }
            sum += time;
            if (time > max) {
                max = time;
            }
        }
        return new Tuple3<>(best, sum / results.size(), max);
    }

    private static class Result {

        private final long totalTime;

        private Result(long totalTime) {
            this.totalTime = totalTime;
        }
    }
}
