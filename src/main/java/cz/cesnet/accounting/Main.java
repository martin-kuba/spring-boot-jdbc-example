package cz.cesnet.accounting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

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

        // rows as list of Maps
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM workload WHERE day>=? LIMIT 5", time);
        rows.forEach(System.out::println);

        // rows as stream of Workload records/classes
        jdbc.queryForStream("SELECT id,day,cpu,jobs_time,utilization_ratio FROM workload WHERE day>=? LIMIT 5", WORKLOAD_ROW_MAPPER, time)
                .forEach(System.out::println);

        // rows as List of Workload instances
        List<Workload> workloads = jdbc.query("SELECT id,day,cpu,jobs_time,utilization_ratio FROM workload WHERE day>=? LIMIT 5", WORKLOAD_ROW_MAPPER, time);
        for (Workload workload : workloads) {
            System.out.println("workload = " + workload);
        }
    }

    record Workload(int id, LocalDate day, int cpu, long jobsTime, float utilizationRatio) {
    }

    static RowMapper<Workload> WORKLOAD_ROW_MAPPER = (rs, rowNum) -> new Workload(
            rs.getInt("id"),
            rs.getDate("day").toLocalDate(),
            rs.getInt("cpu"),
            rs.getLong("jobs_time"),
            rs.getFloat("utilization_ratio")
    );
}
