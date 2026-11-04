package dev.replay;

import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;

final class Schema {
    private Schema() {}

    // Version 0 lets V1 adopt the two existing step-1 tables without losing data.
    static synchronized void migrate(JdbcTemplate jdbc) {
        Flyway.configure().dataSource(jdbc.getDataSource()).baselineOnMigrate(true)
                .baselineVersion("0").locations("classpath:db/migration").load().migrate();
    }
}
