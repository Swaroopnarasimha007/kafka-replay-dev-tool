package dev.replay;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.h2.tools.Server;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerBrokerTest {
    EmbeddedKafkaKraftBroker broker;Server database;Profiles profiles;Planner planner;WorkerStore store;ReplayWorker worker;
    KafkaReplayTransport transport;ObjectMapper json=new ObjectMapper().findAndRegisterModules();String jdbcUrl,profileId;JsonNode fixture;
    Path evidence=Path.of("target/step3-evidence");

    @BeforeAll void setup() throws Exception {
        Files.createDirectories(evidence);
        jdbcUrl=System.getenv("REPLAY_TEST_DB_URL");
        if(jdbcUrl==null||jdbcUrl.isBlank()) {
            database=Server.createTcpServer("-tcpPort","0","-tcpDaemon","-ifNotExists").start();
            jdbcUrl="jdbc:h2:tcp://localhost:"+database.getPort()+"/mem:worker-crash;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        }
        broker=new EmbeddedKafkaKraftBroker(1,3,"orders.DLT","orders");
        broker.brokerProperties(Map.of("transaction.state.log.replication.factor","1","transaction.state.log.min.isr","1","transaction.state.log.num.partitions","1","offsets.topic.replication.factor","1","log.retention.ms","-1"));
        broker.afterPropertiesSet();profiles=WorkerCrashChild.profiles(jdbcUrl);
        var credentials=new Profiles.Input("Crash fixture",broker.getBrokersAsString(),"PLAINTEXT",null,null,null,null,null);
        profileId=profiles.save((String)profiles.test(credentials).get("testToken")).id();
        try(Admin admin=Admin.create(profiles.props(credentials))) {
            admin.createTopics(List.of(new NewTopic("replay-progress",1,(short)1).configs(Map.of("cleanup.policy","delete","retention.ms","-1","retention.bytes","-1")))).all().get(10,TimeUnit.SECONDS);
        }
        fixture=json.readTree(Path.of("docs/fixtures/source-500.json").toFile());
        var config=profiles.props(credentials);config.put("key.serializer",ByteArraySerializer.class);config.put("value.serializer",ByteArraySerializer.class);
        try(var producer=new KafkaProducer<byte[],byte[]>(config)) {for(var row:fixture)PlannerBrokerTest.seed(producer,row);}
        planner=new Planner(profiles,new ReplaySource(profiles,new Browser(profiles)),json);store=new WorkerStore(planner,profiles);
        transport=new KafkaReplayTransport(profiles,planner);worker=new ReplayWorker(store,transport);
    }
    @AfterAll void close() {try{if(broker!=null)broker.destroy();}finally{if(database!=null)database.stop();}}
    Planner.Job approve() {
        var mapping=planner.saveMapping(null,new Planner.MappingInput(profileId,"orders.DLT","orders",Manifest.EXTRACTOR,Manifest.PARTITION_POLICY,
                Manifest.HEADER_POLICY,List.of("retry-attempts","kafka_dlt-original-topic","kafka_dlt-original-partition"),List.of("alice"),true,null),PlannerTest.ADMIN);
        var preview=planner.preview(new Planner.PreviewRequest(mapping.id(),null,100000,10000),PlannerTest.OPERATOR);assertEquals(500,preview.count());
        return planner.approve(PlannerTest.approval(preview,UUID.randomUUID().toString()),PlannerTest.OPERATOR);
    }
    Properties readProperties() {
        var p=profiles.replayProps(profileId);p.put("key.deserializer",ByteArrayDeserializer.class);p.put("value.deserializer",ByteArrayDeserializer.class);
        p.put("enable.auto.commit","false");p.put("isolation.level","read_committed");p.put("allow.auto.create.topics","false");return p;
    }
    Map<TopicPartition,Long> ends(String topic,int count) {
        try(var c=new KafkaConsumer<byte[],byte[]>(readProperties())) {
            var partitions=new ArrayList<TopicPartition>();for(int i=0;i<count;i++)partitions.add(new TopicPartition(topic,i));return c.endOffsets(partitions);
        }
    }
    void step(String id) throws Exception {Thread.sleep(3);worker.runOne(id,ReplayTransport.Hook.NONE);}
    void finish(String id) throws Exception {
        long deadline=System.nanoTime()+Duration.ofSeconds(60).toNanos();
        while(!Set.of("COMPLETED","COMPLETED_WITH_EXCEPTIONS","BLOCKED").contains(store.state(id))&&System.nanoTime()<deadline)step(id);
        assertEquals("COMPLETED",store.state(id),()->String.valueOf(store.execution(id)));
    }
    void crashInSeparateProcess(String id,ReplayTransport.Point point) throws Exception {
        Path argsFile=evidence.resolve("child-"+point+".args");
        String classpath=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")).replace('\\','/');
        String content="-cp\n\""+classpath+"\"\ndev.replay.WorkerCrashChild\n\""+jdbcUrl+"\"\n"+id+"\n"+point.name()+"\n";
        Files.writeString(argsFile,content,StandardCharsets.UTF_8);
        var java=Path.of(System.getProperty("java.home"),"bin",System.getProperty("os.name").startsWith("Windows")?"java.exe":"java");
        Path log=evidence.resolve("child-"+point+".log");
        var child=new ProcessBuilder(java.toString(),"@"+argsFile.toAbsolutePath()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(child.waitFor(50,TimeUnit.SECONDS),"Child timed out: "+log);
            assertEquals(71,child.exitValue(),()->"Crash point not reached; inspect "+log+" / "+store.execution(id));
        } finally {if(child.isAlive())child.destroyForcibly().waitFor();}
        // Expedite deterministic lease expiry instead of waiting a wall-clock minute.
        planner.db.update("UPDATE replay_execution SET lease_until=0 WHERE job_id=?",id);
    }
    @ParameterizedTest @EnumSource(ReplayTransport.Point.class)
    void processCrashMatrixHasExactly500CommittedOutputs(ReplayTransport.Point point) throws Exception {
        var before=ends("orders",3);var job=approve();store.command(job.id(),"start",PlannerTest.OPERATOR);
        crashInSeparateProcess(job.id(),point);
        assertEquals(0,store.execution(job.id()).published(),"No projection committed in the killed child");
        step(job.id());assertEquals("AWAITING_RELEASE",store.state(job.id()),()->String.valueOf(store.execution(job.id())));
        assertEquals(1,store.execution(job.id()).published());assertEquals(1,store.receipts(job.id()).get(0).receipt().outcomes().size());
        store.command(job.id(),"release",PlannerTest.OPERATOR);store.command(job.id(),"release",PlannerTest.OPERATOR);
        // Lose the disposable projection; the next acquisition must reconstruct it from Kafka.
        planner.db.update("DELETE FROM replay_receipt_projection WHERE job_id=?",job.id());
        finish(job.id());var after=ends("orders",3);
        var outputs=verifyOutputs(job,before,after);
        assertEquals(500,store.execution(job.id()).published());assertEquals(0,store.execution(job.id()).skipped());
        var exported=new LinkedHashMap<String,Object>();exported.put("manifest",planner.verifier(job.id(),PlannerTest.OPERATOR));
        exported.put("dashboard",new WorkerMetrics(store).dashboard(job.id(),PlannerTest.OPERATOR));exported.put("outputs",outputs);
        json.writerWithDefaultPrettyPrinter().writeValue(evidence.resolve("execution-"+point+".json").toFile(),exported);
    }
    List<Map<String,Object>> verifyOutputs(Planner.Job job,Map<TopicPartition,Long> before,Map<TopicPartition,Long> after) {
        var actual=new TreeMap<String,ConsumerRecord<byte[],byte[]>>();
        try(var c=new KafkaConsumer<byte[],byte[]>(readProperties())) {
            c.assign(before.keySet());before.forEach(c::seek);long deadline=System.nanoTime()+Duration.ofSeconds(15).toNanos();
            while(before.keySet().stream().anyMatch(tp->c.position(tp)<after.get(tp))) {
                assertTrue(System.nanoTime()<deadline,"Independent output consumer timed out");
                for(var row:c.poll(Duration.ofMillis(100)))if(row.offset()<after.get(new TopicPartition(row.topic(),row.partition())))actual.put(row.partition()+":"+row.offset(),row);
            }
        }
        assertEquals(500,actual.size(),"zero missing, duplicate or unapproved committed output records");
        var expected=planner.storedJob(job.id(),PlannerTest.OPERATOR).preview().entries();var used=new HashSet<String>();var exported=new ArrayList<Map<String,Object>>();
        for(var committed:store.receipts(job.id()))for(var outcome:committed.receipt().outcomes()) {
            String coordinates=outcome.destinationPartition()+":"+outcome.destinationOffset();assertTrue(used.add(coordinates));
            var row=actual.get(coordinates);assertNotNull(row);var entry=expected.get(outcome.position());
            assertArrayEquals(KafkaReplayTransport.bytes(entry.keyBase64()),row.key());assertArrayEquals(KafkaReplayTransport.bytes(entry.valueBase64()),row.value());
            assertEquals(entry.headers(),Browser.row(row,true).headers());
            var result=new LinkedHashMap<String,Object>();result.put("position",outcome.position());result.put("topic",row.topic());result.put("partition",row.partition());result.put("offset",row.offset());
            result.put("keyBase64",Browser.b64(row.key()));result.put("valueBase64",Browser.b64(row.value()));result.put("headers",Browser.row(row,true).headers());exported.add(result);
        }
        assertEquals(actual.keySet(),used);return exported;
    }
    @Test void missingProgressHistoryBlocksRecovery() throws Exception {
        var job=approve();store.command(job.id(),"start",PlannerTest.OPERATOR);step(job.id());
        assertEquals("AWAITING_RELEASE",store.state(job.id()));
        long end=ends("replay-progress",1).get(new TopicPartition("replay-progress",0));
        try(Admin admin=Admin.create(profiles.replayProps(profileId))) {
            admin.deleteRecords(Map.of(new TopicPartition("replay-progress",0),RecordsToDelete.beforeOffset(end))).all().get(10,TimeUnit.SECONDS);
        }
        store.command(job.id(),"release",PlannerTest.OPERATOR);step(job.id());
        assertEquals("BLOCKED",store.state(job.id()));assertTrue(store.execution(job.id()).detail().contains("MISSING_PROGRESS_HISTORY"));
        assertEquals(1,store.execution(job.id()).published());
    }
    @Test void expiredLeaseFencesStaleProducerAndSecondWorkerDoesNotDuplicateCanary() throws Exception {
        var job=approve();store.command(job.id(),"start",PlannerTest.OPERATOR);
        String oldOwner=store.claim(job.id());assertNotNull(oldOwner);assertNull(store.claim(job.id()));
        var plan=store.plan(job.id());var waiting=new CountDownLatch(1);var proceed=new CountDownLatch(1);var executor=Executors.newSingleThreadExecutor();
        try(var old=transport.open(plan)) {
            old.initialize();store.anchor(job.id(),oldOwner,old.newAnchor());
            var validations=old.validate(plan.preview().entries().subList(0,1));
            var stale=executor.submit(()->old.publish(new ReplayTransport.Batch(plan,0,0,"GENESIS",validations),point->{
                if(point==ReplayTransport.Point.BEFORE_COMMIT){waiting.countDown();try{proceed.await(30,TimeUnit.SECONDS);}catch(InterruptedException e){throw new IllegalStateException(e);}}
            }));
            assertTrue(waiting.await(15,TimeUnit.SECONDS));
            planner.db.update("UPDATE replay_execution SET lease_until=0 WHERE job_id=?",job.id());
            var second=new ReplayWorker(store,new KafkaReplayTransport(profiles,planner));second.runOne(job.id(),ReplayTransport.Hook.NONE);
            assertEquals("AWAITING_RELEASE",store.state(job.id()),()->String.valueOf(store.execution(job.id())));
            proceed.countDown();assertThrows(ExecutionException.class,()->stale.get(15,TimeUnit.SECONDS));
            assertEquals(1,store.execution(job.id()).published());
        } finally {proceed.countDown();executor.shutdownNow();}
    }
    @Test void expiredSourceIsAnExplicitCommittedExceptionOutcome() throws Exception {
        try(Admin admin=Admin.create(profiles.replayProps(profileId))) {
            admin.createTopics(List.of(new NewTopic("expired.DLT",3,(short)1),new NewTopic("expired",3,(short)1))).all().get(10,TimeUnit.SECONDS);
        }
        var properties=profiles.replayProps(profileId);properties.put("key.serializer",ByteArraySerializer.class);properties.put("value.serializer",ByteArraySerializer.class);
        try(var producer=new KafkaProducer<byte[],byte[]>(properties)) {
            for(int partition=0;partition<3;partition++) {
                var record=new ProducerRecord<byte[],byte[]>("expired.DLT",partition,new byte[]{(byte)partition},new byte[]{1});
                record.headers().add("kafka_dlt-original-topic","expired".getBytes(StandardCharsets.UTF_8))
                        .add("kafka_dlt-original-partition",java.nio.ByteBuffer.allocate(4).putInt(partition).array());
                producer.send(record).get(10,TimeUnit.SECONDS);
            }
        }
        var mapping=planner.saveMapping(null,new Planner.MappingInput(profileId,"expired.DLT","expired",Manifest.EXTRACTOR,Manifest.PARTITION_POLICY,Manifest.HEADER_POLICY,List.of(),List.of("alice"),true,null),PlannerTest.ADMIN);
        var preview=planner.preview(new Planner.PreviewRequest(mapping.id(),null,100000,1000),PlannerTest.OPERATOR);
        var job=planner.approve(PlannerTest.approval(preview,UUID.randomUUID().toString()),PlannerTest.OPERATOR);
        try(Admin admin=Admin.create(profiles.replayProps(profileId))) {
            admin.deleteRecords(Map.of(new TopicPartition("expired.DLT",0),RecordsToDelete.beforeOffset(1))).all().get(10,TimeUnit.SECONDS);
        }
        store.command(job.id(),"start",PlannerTest.OPERATOR);step(job.id());
        assertEquals("AWAITING_RELEASE",store.state(job.id()));assertEquals(1,store.execution(job.id()).skipped());
        assertEquals("SKIPPED_EXPIRED",store.receipts(job.id()).get(0).receipt().outcomes().get(0).status());
        store.command(job.id(),"release",PlannerTest.OPERATOR);step(job.id());
        assertEquals("COMPLETED_WITH_EXCEPTIONS",store.state(job.id()),()->String.valueOf(store.execution(job.id())));
        assertEquals(2,store.execution(job.id()).published());
    }
    @Test void unrelatedOpenTransactionCannotHideCommittedProgressAndCauseDuplicateOutput() throws Exception {
        var job=approve();store.command(job.id(),"start",PlannerTest.OPERATOR);step(job.id());
        store.command(job.id(),"release",PlannerTest.OPERATOR);
        var producerConfig=profiles.replayProps(profileId);producerConfig.put("key.serializer",ByteArraySerializer.class);producerConfig.put("value.serializer",ByteArraySerializer.class);
        producerConfig.put("transactional.id","unrelated-barrier-"+UUID.randomUUID());producerConfig.put("transaction.timeout.ms","30000");
        var env=(org.springframework.mock.env.MockEnvironment)profiles.env;
        try(var barrier=new KafkaProducer<byte[],byte[]>(producerConfig)) {
            barrier.initTransactions();barrier.beginTransaction();
            barrier.send(new ProducerRecord<>("replay-progress",0,"unrelated".getBytes(StandardCharsets.UTF_8),"{}".getBytes(StandardCharsets.UTF_8))).get(10,TimeUnit.SECONDS);
            try(var session=transport.open(store.plan(job.id()))) {
                session.initialize();
                var plan=store.plan(job.id());var validation=session.validate(plan.preview().entries().subList(1,2));
                session.publish(new ReplayTransport.Batch(plan,1,1,store.receipts(job.id()).get(0).hash(),validation),ReplayTransport.Hook.NONE);
            }
            env.withProperty("replay.stability-timeout-ms","1000");
            var before=ends("orders",3);step(job.id());
            assertEquals("BLOCKED",store.state(job.id()));assertTrue(store.execution(job.id()).detail().startsWith("PROGRESS_NOT_STABLE"));
            assertEquals(before,ends("orders",3),"No republish while a committed receipt is hidden");
            barrier.abortTransaction();store.command(job.id(),"resume",PlannerTest.OPERATOR);finish(job.id());
            assertEquals(500,store.execution(job.id()).published());
        } finally {env.withProperty("replay.stability-timeout-ms","10000");}
    }
    @Test void ambiguousCommitAcknowledgmentReconcilesBeforePublishingAgain() throws Exception {
        var job=approve();store.command(job.id(),"start",PlannerTest.OPERATOR);
        worker.runOne(job.id(),point->{if(point==ReplayTransport.Point.AFTER_COMMIT_BEFORE_PROJECTION)throw new org.apache.kafka.common.errors.TimeoutException("Injected acknowledgment loss");});
        assertEquals("BLOCKED",store.state(job.id()));assertEquals(0,store.execution(job.id()).published());
        var before=ends("orders",3);store.command(job.id(),"resume",PlannerTest.OPERATOR);step(job.id());
        assertEquals("AWAITING_RELEASE",store.state(job.id()));assertEquals(1,store.execution(job.id()).published());
        assertEquals(before,ends("orders",3));
    }
}
