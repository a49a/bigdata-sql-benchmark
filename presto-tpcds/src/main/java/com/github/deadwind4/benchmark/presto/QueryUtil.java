package com.github.deadwind4.benchmark.presto;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

class QueryUtil {

    static LinkedHashMap<String, String> getQueries(String location, String queries) {
        LinkedHashMap<String, Supplier<InputStream>> sql = new LinkedHashMap<>();
        List<String> queryList = queries == null ? null : Arrays.asList(queries.split(","));
        if (location == null) {
            for (int i = 1; i < 100; i++) {
                String name = "q" + i + ".sql";
                ClassLoader cl = Benchmark.class.getClassLoader();
                String path = "queries/" + name;
                if (cl.getResource(path) == null) {
                    String a = "q" + i + "a.sql";
                    sql.put(a, () -> cl.getResourceAsStream("queries/" + a));
                    String b = "q" + i + "b.sql";
                    sql.put(b, () -> cl.getResourceAsStream("queries/" + b));
                } else {
                    sql.put(name, () -> cl.getResourceAsStream(path));
                }
            }
        } else {
            Stream<File> files = queryList == null ?
                    Arrays.stream(requireNonNull(new File(location).listFiles())) :
                    queryList.stream().map(file -> new File(location, file));
            files.forEach(file -> sql.put(file.getName(), () -> {
                try {
                    return new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    return null;
                }
            }));
        }
        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
        sql.forEach((name, supplier) -> {
            if (queryList == null || queryList.contains(name)) {
                InputStream in = supplier.get();
                if (in != null) {
                    ret.put(name, streamToString(in));
                }
            }
        });
        return ret;
    }

    private static String streamToString(InputStream inputStream) {
        BufferedInputStream in = new BufferedInputStream(inputStream);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            int c;
            while ((c = in.read()) != -1) {
                outStream.write(c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        return new String(outStream.toByteArray(), StandardCharsets.UTF_8);
    }
}
