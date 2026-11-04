package dev.replay;

import java.util.Base64;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Separate JVM used only by the real-broker crash acceptance test. Never packaged in the release. */
public class WorkerCrashChild {
    static MockEnvironment environment() {
        var env=new MockEnvironment().withProperty("replay.encryption-key",Base64.getEncoder().encodeToString(new byte[32]))
                .withProperty("replay.allowed-hosts","localhost,127.0.0.1").withProperty("replay.allow-plaintext","true")
                .withProperty("replay.topic-pattern",".*DLT").withProperty("replay.worker-enabled","true")
                .withProperty("replay.scheduler-enabled","false").withProperty("replay.receipts-topic","replay-progress")
                .withProperty("replay.batch-size","100");
        env.setActiveProfiles("live");return env;
    }
    static Profiles profiles(String jdbcUrl) {
        String user=System.getenv().getOrDefault("REPLAY_TEST_DB_USER","sa");
        String password=System.getenv().getOrDefault("REPLAY_TEST_DB_PASSWORD","");
        var db=new JdbcTemplate(new DriverManagerDataSource(jdbcUrl,user,password));
        var beans=new StaticListableBeanFactory();beans.addBean("jdbc",db);
        return new Profiles(environment(),beans.getBeanProvider(JdbcTemplate.class),new ObjectMapper());
    }
    public static void main(String[] args) {
        var profiles=profiles(args[0]);var planner=new Planner(profiles,new ReplaySource(profiles,new Browser(profiles)),new ObjectMapper());
        var store=new WorkerStore(planner,profiles);var worker=new ReplayWorker(store,new KafkaReplayTransport(profiles,planner));
        var point=ReplayTransport.Point.valueOf(args[2]);
        worker.runOne(args[1],observed->{if(observed==point)Runtime.getRuntime().halt(71);});
        System.err.println("Crash point was not reached: "+store.execution(args[1]));
        System.exit(2);
    }
}
