package spark3.sql;

import org.apache.hudi.com.beust.jcommander.Parameter;

import java.io.Serializable;

public class CliOptions implements Serializable {

    @Parameter(names = {"-q", "--queries"},
            description = "sql query names. If the value is 'all', all queries will be executed.",
            required = true)
    public String queries;

    @Parameter(names = {"-d", "--database"},
            description = "sql query names. If the value is 'all', all queries will be executed.",
            required = true)
    public String database;
}
