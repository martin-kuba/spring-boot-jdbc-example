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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.util.function.ThrowingConsumer;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Date;

@SpringBootApplication
@Component
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
        Timestamp time = new Timestamp(ZonedDateTime.now().minusDays(3).toInstant().toEpochMilli());

        log.info("rows as list of Maps");
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM workload WHERE day>=? LIMIT 5", time);
        for (Map<String, Object> row : rows) {
            Integer id = (Integer) row.get("id");
            java.sql.Date day = (java.sql.Date) row.get("day");
            BigDecimal utilizationRatio = (BigDecimal) row.get("utilization_ratio");
            log.info("id={} day={} utilization_ratio={}%", id, day, utilizationRatio);
        }

        log.info("rows as List of Workload instances");
        List<Workload> workloads = jdbc.query("SELECT id,day,cpu,jobs_time,utilization_ratio FROM workload WHERE day>=? LIMIT 5", WORKLOAD_ROW_MAPPER, time);
        // write rows into a CSV file
        try (SequenceWriter sw = csvSequenceWriter(Workload.class, "workloads.csv")) {
            for (Workload workload : workloads) {
                sw.write(workload);
            }
        }

        log.info("rows as stream of Job instances");
        try (SequenceWriter sw = csvSequenceWriter(Job.class, "jobs.csv")) {
            jdbc.queryForStream("SELECT acct_id_string, jobname, queue, start_time, end_time FROM acct_pbs_record LIMIT 2000000", JOB_ROW_MAPPER)
                    .forEach(ThrowingConsumer.of(sw::write));
        }
    }

    record Workload(int id, LocalDate day, int cpu, long jobsTime, float utilizationRatio) {
    }

    final static RowMapper<Workload> WORKLOAD_ROW_MAPPER = (rs, rowNum) -> new Workload(
            rs.getInt("id"),
            rs.getObject("day", LocalDate.class),
            rs.getInt("cpu"),
            rs.getLong("jobs_time"),
            rs.getFloat("utilization_ratio")
    );

    /**
     * Record for keeping job data.
     */
    record Job(String id, String jobname, String queue, Date start_time, Date end_time) {
    }

    /**
     * Lambda that maps ResultSet rows into Job instances.
     */
    final static RowMapper<Job> JOB_ROW_MAPPER = (rs, rowNum) -> new Job(
            rs.getString("acct_id_string"),
            rs.getString("jobname"),
            rs.getString("queue"),
            new Date(rs.getLong("start_time") * 1000L),
            new Date(rs.getLong("end_time") * 1000L)
    );

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
