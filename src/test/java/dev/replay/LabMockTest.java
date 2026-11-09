package dev.replay;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.datasource.url=jdbc:h2:mem:labmock;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
    "replay.lab-password=test-password-for-lab",
    "replay.scheduler-enabled=false"
})
@ActiveProfiles("lab")
class LabMockTest {
    @Autowired Profiles profiles;
    @Autowired Browser browser;
    @Autowired ReplaySource source;

    @Test void mockRemainsAvailableAlongsidePersistedRealConnections() {
        profiles.db.update("INSERT INTO cluster_profiles VALUES (?,?,?,?,?)",
                "external", "External cluster", "localhost:9092", "real-cluster", "PLAINTEXT");
        assertTrue(profiles.list().stream().anyMatch(p -> p.id().equals("demo")));
        assertTrue(profiles.list().stream().anyMatch(p -> p.id().equals("external")));
        assertFalse(profiles.real("demo"));
        assertTrue(profiles.real("external"));
        assertEquals("demo-cluster", source.identity("demo", "orders.DLT", "orders").clusterId());
        var page = browser.browse("demo", "orders.DLT",
                new Browser.Query(null, null, null, null, null, null, null, 100, 100), false);
        assertEquals(60, page.records().size());
    }
}
