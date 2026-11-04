package dev.replay;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.access.AccessDeniedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.*;

class PlannerTest {
    static final Planner.Actor ADMIN = new Planner.Actor("admin",Set.of("Admin"));
    static final Planner.Actor OPERATOR = new Planner.Actor("alice",Set.of("Operator"));
    static final Planner.Actor VIEWER = new Planner.Actor("viewer",Set.of("Viewer"));
    Profiles profiles; Planner planner; ReplaySource source; JdbcTemplate db;

    @BeforeEach void setup() {
        var env=new MockEnvironment().withProperty("replay.topic-pattern",".*DLT"); env.setActiveProfiles("demo");
        db=new JdbcTemplate(new DriverManagerDataSource("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=PostgreSQL;DB_CLOSE_DELAY=-1","sa",""));
        var beans=new StaticListableBeanFactory();beans.addBean("jdbc",db);
        profiles=new Profiles(env,beans.getBeanProvider(JdbcTemplate.class),new ObjectMapper());
        source=new ReplaySource(profiles,new Browser(profiles));planner=new Planner(profiles,source,new ObjectMapper());
    }
    static Planner.MappingInput input(Integer expected,List<String> subjects) {
        return new Planner.MappingInput("demo","orders.DLT","orders",Manifest.EXTRACTOR,Manifest.PARTITION_POLICY,
                Manifest.HEADER_POLICY,List.of("kafka_dlt-original-topic","kafka_dlt-original-partition"),subjects,true,expected);
    }
    Planner.Mapping mapping() { return planner.saveMapping(null,input(null,List.of()),ADMIN); }
    Planner.PreviewView preview(Planner.Mapping m,int cap) {
        return planner.preview(new Planner.PreviewRequest(m.id(),null,100,cap),OPERATOR);
    }
    static Planner.Approval approval(Planner.PreviewView p,String key) {
        return new Planner.Approval(p.id(),p.digest(),key,"Investigated and approved",true,false,null);
    }

    @Test void concurrentApprovalWritesOneJobAndOneEventAndReloads() throws Exception {
        var p=preview(mapping(),1000);var request=approval(p,"identical-request");
        var executor=Executors.newFixedThreadPool(8);
        try {
            var barrier=new CyclicBarrier(8);var futures=new ArrayList<Future<Planner.Job>>();
            for(int i=0;i<8;i++)futures.add(executor.submit(()->{barrier.await();return planner.approve(request,OPERATOR);}));
            var ids=new HashSet<String>();for(var f:futures)ids.add(f.get(20,TimeUnit.SECONDS).id());
            assertEquals(1,ids.size());assertEquals(1,db.queryForObject("SELECT count(*) FROM replay_jobs",Integer.class));
            assertEquals(1,db.queryForObject("SELECT count(*) FROM replay_events",Integer.class));
            assertEquals(1,db.queryForObject("SELECT count(*) FROM replay_previews",Integer.class));
            var restarted=new Planner(profiles,source,new ObjectMapper());var job=restarted.job(ids.iterator().next(),OPERATOR);
            assertEquals(60,job.count());assertEquals(p.digest(),job.digest());assertEquals(p.mapping(),job.mapping());
            assertEquals("APPROVED",job.state());assertTrue(job.workerEnabled());
        } finally {executor.shutdownNow();}
    }
    @Test void conflictingKeyAndStaleMappingsRequireReview() {
        var m=mapping();var first=preview(m,1000);planner.approve(approval(first,"same-key"),OPERATOR);
        var second=preview(m,1000);assertThrows(Planner.Conflict.class,()->planner.approve(approval(second,"same-key"),OPERATOR));
        planner.saveMapping(m.id(),input(1,List.of()),ADMIN);
        assertThrows(Planner.Conflict.class,()->planner.approve(approval(second,"new-key"),OPERATOR));
        assertEquals(2,planner.mapping(m.id()).version());assertEquals(2,db.queryForObject("SELECT count(*) FROM replay_mappings",Integer.class));
    }
    @Test void partialRequiresExplicitExactSelectionAndZeroMatchesCannotApprove() {
        var m=mapping();var partial=preview(m,3);assertTrue(partial.partial());
        assertThrows(IllegalArgumentException.class,()->planner.approve(approval(partial,"partial"),OPERATOR));
        var positions=partial.records().stream().map(Planner.Coordinate::position).toList();
        var selected=new Planner.Approval(partial.id(),partial.digest(),"partial","Select only listed positions",true,true,positions);
        assertEquals(3,planner.approve(selected,OPERATOR).count());
        var q=new Browser.Query(null,null,null,"bm8tbWF0Y2g=",null,null,null,null,null);
        var empty=planner.preview(new Planner.PreviewRequest(m.id(),q,10,1000),OPERATOR);assertEquals(0,empty.count());
        assertThrows(IllegalArgumentException.class,()->planner.approve(approval(empty,"empty"),OPERATOR));
    }
    @Test void roleAndMappingAuthorizationAreRechecked() {
        assertThrows(AccessDeniedException.class,()->planner.saveMapping(null,input(null,List.of()),OPERATOR));
        var m=planner.saveMapping(null,input(null,List.of("alice")),ADMIN);
        assertThrows(AccessDeniedException.class,()->planner.preview(new Planner.PreviewRequest(m.id(),null,10,100),VIEWER));
        assertThrows(AccessDeniedException.class,()->planner.preview(new Planner.PreviewRequest(m.id(),null,10,100),new Planner.Actor("bob",Set.of("Operator"))));
        var p=preview(m,100);planner.saveMapping(m.id(),input(1,List.of("bob")),ADMIN);
        assertThrows(AccessDeniedException.class,()->planner.approve(approval(p,"revoked"),OPERATOR));
        var wrapped=new Planner.MappingInput("demo","orders.DLT","orders","WRAPPED_JSON_V1",Manifest.PARTITION_POLICY,Manifest.HEADER_POLICY,List.of(),List.of(),true,null);
        assertThrows(IllegalArgumentException.class,()->planner.saveMapping(null,wrapped,ADMIN));
    }
    @Test void extractionPreservesNullsBinaryAndRepeatedHeaders() {
        var m=mapping();var p=preview(m,1000);var entries=planner.loadPreview(p.id(),OPERATOR).entries();
        assertEquals("AP8B",entries.get(3).valueBase64());assertNull(entries.get(4).valueBase64());assertNull(entries.get(5).keyBase64());
        assertEquals(2,entries.get(0).headers().stream().filter(h->h.name().equals("trace")).count());
        assertFalse(entries.get(0).headers().stream().anyMatch(h->m.removeHeaders().contains(h.name())));
        var bad=new Browser.Row("orders.DLT",0,0,0,null,null,List.of(),false);
        assertThrows(IllegalArgumentException.class,()->Manifest.extract(bad,m,0));
        assertThrows(Planner.Conflict.class,()->new WorkerStore(planner,profiles).command(planner.approve(approval(p,"not-started"),OPERATOR).id(),"release",OPERATOR));
    }
    @Test void stateContractIsTerminalAndReleaseIsIdempotent() {
        assertEquals(JobStates.State.RUNNING,JobStates.release(JobStates.State.AWAITING_RELEASE));
        assertEquals(JobStates.State.RUNNING,JobStates.release(JobStates.State.RUNNING));
        assertEquals(JobStates.State.PAUSED,JobStates.release(JobStates.State.PAUSED,true));
        assertThrows(Planner.Conflict.class,()->JobStates.release(JobStates.State.APPROVED));
        assertThrows(Planner.Conflict.class,()->JobStates.require(JobStates.State.COMPLETED,JobStates.State.RUNNING));
    }
    @Test void changedTopicIdentityRequiresNewMappingVersion() {
        var m=mapping();
        var replaced=new ReplaySource(profiles,new Browser(profiles)) {
            @Override Identity identity(String profile,String from,String target) {
                return new Identity("demo-cluster","replacement-topic-id","demo-orders-v1",3);
            }
        };
        var service=new Planner(profiles,replaced,new ObjectMapper());
        assertThrows(Planner.Conflict.class,()->service.preview(new Planner.PreviewRequest(m.id(),null,10,100),OPERATOR));
    }
    @Test void sparseOffsetsAreManifestCoordinatesNotAnAssumedRange() {
        var m=mapping();
        var original=source.scan("demo","orders.DLT",new Browser.Query(0,null,null,null,null,null,null,null,null),3);
        var sparseRows=new ArrayList<Browser.Row>();
        for(int i=0;i<original.records().size();i++) {
            var r=original.records().get(i);
            sparseRows.add(new Browser.Row(r.topic(),0,2L+i*11,r.timestamp(),r.keyBase64(),r.valueBase64(),r.headers(),false));
        }
        var sparse=new ReplaySource(profiles,new Browser(profiles)) {
            @Override Scan scan(String profile,String topic,Browser.Query filter,int budget) {
                return new Scan(sparseRows,Map.of(0,0L),Map.of(0,30L),Map.of(0,30L),3,false);
            }
        };
        var service=new Planner(profiles,sparse,new ObjectMapper());
        var p=service.preview(new Planner.PreviewRequest(m.id(),null,10,100),OPERATOR);
        assertEquals(List.of(2L,13L,24L),p.records().stream().map(Planner.Coordinate::offset).toList());
        var job=service.approve(approval(p,"sparse"),OPERATOR);
        assertEquals(3,job.count());
    }
    @Test void migrationsAdoptExistingStep1TablesWithoutDroppingProfiles() {
        var legacy=new JdbcTemplate(new DriverManagerDataSource("jdbc:h2:mem:legacy-"+UUID.randomUUID()+";MODE=PostgreSQL;DB_CLOSE_DELAY=-1","sa",""));
        legacy.execute("CREATE TABLE cluster_profiles(id varchar(36) PRIMARY KEY,name text NOT NULL,bootstrap text NOT NULL,cluster_id text NOT NULL,secret text NOT NULL)");
        legacy.update("INSERT INTO cluster_profiles VALUES ('old','Existing profile','localhost:9092','cluster','encrypted-secret')");
        Schema.migrate(legacy);
        assertEquals("Existing profile",legacy.queryForObject("SELECT name FROM cluster_profiles WHERE id='old'",String.class));
        assertEquals(0,legacy.queryForObject("SELECT count(*) FROM replay_jobs",Integer.class));
        Schema.migrate(legacy);
        assertEquals(1,legacy.queryForObject("SELECT count(*) FROM cluster_profiles",Integer.class));
    }
}
