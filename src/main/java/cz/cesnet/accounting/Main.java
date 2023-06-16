package cz.cesnet.accounting;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.function.ThrowingConsumer;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class Main implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private JdbcTemplate jdbc;

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("starting");
        ZonedDateTime zonedDateTime1 = ZonedDateTime.parse("2023-06-01T12:00:00+02:00");
        Timestamp timestamp1 = new Timestamp(zonedDateTime1.toInstant().toEpochMilli());
        long linuxTime1 = zonedDateTime1.toInstant().getEpochSecond();
        long linuxTime2 = zonedDateTime1.plusMinutes(1).toInstant().getEpochSecond();

        log.info("rows as list of Maps");
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM workload WHERE day>=? LIMIT 5", timestamp1);
        for (Map<String, Object> row : rows) {
            Integer id = (Integer) row.get("id");
            java.sql.Date day = (java.sql.Date) row.get("day");
            BigDecimal utilizationRatio = (BigDecimal) row.get("utilization_ratio");
            log.info("id={} day={} utilization_ratio={}%", id, day, utilizationRatio);
        }

        log.info("rows as List of Workload instances");
        List<Workload> workloads = jdbc.query(
                "SELECT id,day,cpu,jobs_time,utilization_ratio FROM workload WHERE day>=? LIMIT 5",
                new DataClassRowMapper<>(Workload.class),
                timestamp1);
        // write rows into a CSV file
        try (SequenceWriter sw = csvSequenceWriter(Workload.class, "workloads.csv")) {
            for (Workload workload : workloads) {
                sw.write(workload);
            }
        }

        log.info("rows as stream of Job instances");
        try (SequenceWriter sw = csvSequenceWriter(Job.class, "jobs.csv")) {
            jdbc.queryForStream("""
                                    SELECT
                                        acct_id_string,
                                        jobname,
                                        queue,
                                        to_timestamp(start_time) as start_time,
                                        to_timestamp(end_time) as end_time
                                    FROM acct_pbs_record
                                    WHERE start_time>?
                                    ORDER BY start_time, acct_id_string
                                    LIMIT 17
                                    """,
                            new DataClassRowMapper<>(Job.class),
                            linuxTime1
                    )
                    .forEach(ThrowingConsumer.of(sw::write));
        }

        log.info("get a single value as query result");
        Long jobsCount = jdbc.queryForObject("SELECT count(*) FROM acct_pbs_record WHERE start_time BETWEEN ? AND ?",
                Long.class, linuxTime1, linuxTime2);
        log.info("jobs count {}", jobsCount);

        log.info("get job ids");
        List<String> ids = jdbc.queryForList("SELECT acct_id_string FROM acct_pbs_record WHERE start_time BETWEEN ? AND ?",
                String.class, linuxTime1, linuxTime2);

        log.info("get jobs from array of ids");
        try (SequenceWriter sw = csvSequenceWriter(Job.class, "jobsByIds.csv")) {
            List<Job> jobs = jdbc.query("""
                            SELECT
                                acct_id_string,
                                jobname,
                                queue,
                                to_timestamp(start_time) as start_time,
                                to_timestamp(end_time) as end_time
                                FROM acct_pbs_record
                                WHERE acct_id_string = ANY(?)
                                ORDER BY start_time, acct_id_string
                            """,
                    pst -> pst.setArray(1, pst.getConnection().createArrayOf(JDBCType.VARCHAR.name(), ids.toArray(new String[0]))),
                    new DataClassRowMapper<>(Job.class));
            jobs.forEach(ThrowingConsumer.of(sw::write));
        }
    }

    record Workload(int id, LocalDate day, int cpu, long jobsTime, float utilizationRatio) {
    }

    /**
     * Record for keeping job data.
     */
    record Job(String acct_id_string, String jobname, String queue, Date start_time, Date end_time) {
    }

    /**
     * Creates a SequenceWriter for writing rows into a CSV file with columns named after the provided class.
     *
     * @param clazz    class for creating CSV file schema
     * @param filename name of CSV file
     * @return SequenceWriter that needs to be closed with try-with-resources
     */
    private SequenceWriter csvSequenceWriter(Class<?> clazz, String filename) throws IOException {
        CsvMapper mapper = CsvMapper
                .builder()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .findAndAddModules()
                .build();
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"));
        return mapper.writer(mapper.typedSchemaFor(clazz).withHeader()).writeValues(new File(filename));
    }


}
