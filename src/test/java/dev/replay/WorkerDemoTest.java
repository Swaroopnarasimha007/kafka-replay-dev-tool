package dev.replay;

import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

class WorkerDemoTest {
    PlannerTest fixture;WorkerStore store;ReplayWorker worker;Planner.Job job;
    @BeforeEach void setup() {
        fixture=new PlannerTest();fixture.setup();store=new WorkerStore(fixture.planner,fixture.profiles);
        worker=new ReplayWorker(store,new KafkaReplayTransport(fixture.profiles,fixture.planner));
        var p=fixture.planner.preview(new Planner.PreviewRequest(fixture.mapping().id(),null,100000,1000),PlannerTest.OPERATOR);
        job=fixture.planner.approve(PlannerTest.approval(p,UUID.randomUUID().toString()),PlannerTest.OPERATOR);
    }
    void run() throws Exception { Thread.sleep(5);worker.runOne(job.id(),ReplayTransport.Hook.NONE); }
    @Test void canaryReleasePauseResumeAndMetricsUseCommittedReceipts() throws Exception {
        assertEquals(0,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
        store.command(job.id(),"start",PlannerTest.OPERATOR);run();
        assertEquals("AWAITING_RELEASE",store.state(job.id()));assertEquals(1,store.execution(job.id()).published());
        store.command(job.id(),"release",PlannerTest.OPERATOR);
        store.command(job.id(),"pause",PlannerTest.OPERATOR);run();assertEquals("PAUSED",store.state(job.id()));
        store.command(job.id(),"release",PlannerTest.OPERATOR);assertEquals("PAUSED",store.state(job.id()));
        store.command(job.id(),"start",PlannerTest.OPERATOR);assertEquals("PAUSED",store.state(job.id()));
        store.command(job.id(),"resume",PlannerTest.OPERATOR);run();run();
        assertEquals("COMPLETED",store.state(job.id()));assertEquals(60,store.execution(job.id()).published());
        assertEquals(60,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
        var metrics=new WorkerMetrics(store).dashboard(job.id(),PlannerTest.OPERATOR);assertEquals(60,metrics.get("published"));
        assertEquals("COMPLETED",fixture.planner.job(job.id(),PlannerTest.OPERATOR).state());
        assertEquals(1,fixture.db.queryForObject("SELECT count(*) FROM replay_events WHERE event_type='RELEASED'",Integer.class));
    }
    static final class Crash extends Error {}
    @Test void committedSyntheticAuthorityRebuildsLostProjectionWithoutDuplicateOutput() throws Exception {
        store.command(job.id(),"start",PlannerTest.OPERATOR);
        assertThrows(Crash.class,()->worker.runOne(job.id(),point->{if(point==ReplayTransport.Point.AFTER_COMMIT_BEFORE_PROJECTION)throw new Crash();}));
        assertEquals(1,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
        assertEquals(0,fixture.db.queryForObject("SELECT count(*) FROM replay_receipt_projection",Integer.class));
        fixture.db.update("UPDATE replay_execution SET lease_until=0 WHERE job_id=?",job.id());run();
        assertEquals("AWAITING_RELEASE",store.state(job.id()));assertEquals(1,store.execution(job.id()).published());
        assertEquals(1,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
    }
    @Test void revokedMappingAndExpiredAuthorizationBlockBeforePublication() throws Exception {
        store.command(job.id(),"start",PlannerTest.OPERATOR);
        fixture.db.update("UPDATE replay_execution SET authorized_until=0 WHERE job_id=?",job.id());run();
        assertEquals("BLOCKED",store.state(job.id()));assertTrue(store.execution(job.id()).detail().startsWith("AUTHORIZATION_EXPIRED"));
        store.command(job.id(),"resume",PlannerTest.OPERATOR);
        var m=job.mapping();fixture.planner.saveMapping(m.id(),PlannerTest.input(1,List.of("bob")),PlannerTest.ADMIN);run();
        assertEquals("BLOCKED",store.state(job.id()));assertEquals(0,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
    }
    @Test void oneRecordCanaryRequiresReleaseButNeverPublishesTwice() throws Exception {
        var filter=new Browser.Query(0,1735689600000L,1735689600000L,null,null,null,null,null,null);
        var p=fixture.planner.preview(new Planner.PreviewRequest(job.mapping().id(),filter,100000,1000),PlannerTest.OPERATOR);
        assertEquals(1,p.count());job=fixture.planner.approve(PlannerTest.approval(p,"one-record"),PlannerTest.OPERATOR);
        store.command(job.id(),"start",PlannerTest.OPERATOR);run();assertEquals("AWAITING_RELEASE",store.state(job.id()));
        store.command(job.id(),"release",PlannerTest.OPERATOR);run();assertEquals("COMPLETED",store.state(job.id()));
        assertEquals(1,store.execution(job.id()).published());assertEquals(1,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
    }
    @Test void rateLimitPreventsImmediateSecondBatchWithoutCanary() throws Exception {
        var p=fixture.planner.preview(new Planner.PreviewRequest(job.mapping().id(),null,1,1000),PlannerTest.OPERATOR);
        job=fixture.planner.approve(new Planner.Approval(p.id(),p.digest(),"slow","Test rate cap",false,false,null),PlannerTest.OPERATOR);
        store.command(job.id(),"start",PlannerTest.OPERATOR);run();assertEquals(1,store.execution(job.id()).published());
        assertFalse(worker.runOne(job.id(),ReplayTransport.Hook.NONE));assertEquals(1,store.execution(job.id()).published());
    }
    @Test void corruptedManifestFailsWithoutAnyOutput() throws Exception {
        store.command(job.id(),"start",PlannerTest.OPERATOR);
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper();
        var document=mapper.readTree(fixture.db.queryForObject("SELECT document FROM replay_previews WHERE id=?",String.class,job.previewId()));
        ((com.fasterxml.jackson.databind.node.ObjectNode)document.get("entries").get(0)).put("valueBase64","AA==");
        fixture.db.update("UPDATE replay_previews SET document=? WHERE id=?",mapper.writeValueAsString(document),job.previewId());
        run();assertEquals("FAILED",store.state(job.id()));
        assertEquals(0,fixture.db.queryForObject("SELECT count(*) FROM demo_outputs",Integer.class));
        assertThrows(Planner.Conflict.class,()->store.command(job.id(),"resume",PlannerTest.OPERATOR));
    }
    @Test void changedMappingCannotReleaseAnOldCanary() throws Exception {
        store.command(job.id(),"start",PlannerTest.OPERATOR);run();assertEquals("AWAITING_RELEASE",store.state(job.id()));
        fixture.planner.saveMapping(job.mapping().id(),PlannerTest.input(1,List.of()),PlannerTest.ADMIN);
        assertThrows(Planner.Conflict.class,()->store.command(job.id(),"release",PlannerTest.OPERATOR));
        assertEquals("AWAITING_RELEASE",store.state(job.id()));assertEquals(1,store.execution(job.id()).published());
    }
}
