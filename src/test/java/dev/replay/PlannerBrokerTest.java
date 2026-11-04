package dev.replay;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import static org.junit.jupiter.api.Assertions.*;

class PlannerBrokerTest {
    @Test void frozen500Exclude40NewRecordsAndApprovalWritesNothingToDestination() throws Exception {
        var broker=new EmbeddedKafkaKraftBroker(1,3,"orders.DLT","orders");broker.afterPropertiesSet();
        try {
            var env=new MockEnvironment().withProperty("replay.encryption-key",Base64.getEncoder().encodeToString(new byte[32]))
                    .withProperty("replay.allowed-hosts","localhost,127.0.0.1").withProperty("replay.allow-plaintext","true").withProperty("replay.topic-pattern",".*DLT");env.setActiveProfiles("live");
            var db=new JdbcTemplate(new DriverManagerDataSource("jdbc:h2:mem:plannerbroker;MODE=PostgreSQL;DB_CLOSE_DELAY=-1","sa",""));
            var beans=new StaticListableBeanFactory();beans.addBean("jdbc",db);var json=new ObjectMapper();
            var profiles=new Profiles(env,beans.getBeanProvider(JdbcTemplate.class),json);
            var credentials=new Profiles.Input("Planner fixture",broker.getBrokersAsString(),"PLAINTEXT",null,null,null,null,null);
            var profile=profiles.save((String)profiles.test(credentials).get("testToken"));
            var input=profiles.props(credentials);input.put("key.serializer",ByteArraySerializer.class);input.put("value.serializer",ByteArraySerializer.class);
            var fixture=json.readTree(Path.of("docs/fixtures/source-500.json").toFile());
            try(var producer=new KafkaProducer<byte[],byte[]>(input)) {
                for(var row:fixture) seed(producer,row);
                producer.flush();
                var source=new ReplaySource(profiles,new Browser(profiles));var planner=new Planner(profiles,source,json);
                var mapping=planner.saveMapping(null,new Planner.MappingInput(profile.id(),"orders.DLT","orders",Manifest.EXTRACTOR,
                        Manifest.PARTITION_POLICY,Manifest.HEADER_POLICY,List.of("kafka_dlt-original-topic","kafka_dlt-original-partition","retry-attempts"),List.of("alice"),true,null),PlannerTest.ADMIN);
                var preview=planner.preview(new Planner.PreviewRequest(mapping.id(),null,250,10000),PlannerTest.OPERATOR);
                assertEquals(500,preview.count());assertFalse(preview.partial());
                // Add 40 subsequent matches after the immutable preview has been stored.
                for(int i=0;i<40;i++) {
                    var original=fixture.get(i);int partition=original.get("partition").asInt();
                    var record=new ProducerRecord<byte[],byte[]>("orders.DLT",partition,original.get("timestamp").asLong(),bytes(original.get("keyBase64")),bytes(original.get("valueBase64")));
                    for(var h:original.get("headers"))record.headers().add(h.get("name").asText(),bytes(h.get("base64")));
                    producer.send(record).get(10,TimeUnit.SECONDS);
                }
                var job=planner.approve(PlannerTest.approval(preview,"fixture-approval"),PlannerTest.OPERATOR);
                assertEquals(500,job.count());assertEquals(preview.digest(),job.digest());
                var stored=planner.verifier(job.id(),PlannerTest.OPERATOR);
                var expected=new HashSet<String>();fixture.forEach(row->expected.add(row.get("partition").asInt()+":"+row.get("offset").asLong()));
                var actual=new HashSet<String>();stored.preview().entries().forEach(row->actual.add(row.partition()+":"+row.offset()));assertEquals(expected,actual);
                // Verify destination end offsets independently, not by inspecting planner implementation.
                var read=profiles.props(credentials);read.put("key.deserializer",ByteArrayDeserializer.class);read.put("value.deserializer",ByteArrayDeserializer.class);read.put("enable.auto.commit","false");read.put("isolation.level","read_committed");
                try(var consumer=new KafkaConsumer<byte[],byte[]>(read)) {
                    var destinations=List.of(new TopicPartition("orders",0),new TopicPartition("orders",1),new TopicPartition("orders",2));
                    assertTrue(consumer.endOffsets(destinations).values().stream().allMatch(offset->offset==0));
                }
                var restarted=new Planner(new Profiles(env,beans.getBeanProvider(JdbcTemplate.class),json),source,json);
                assertEquals(job,restarted.job(job.id(),PlannerTest.OPERATOR));
                assertEquals(540,planner.preview(new Planner.PreviewRequest(mapping.id(),null,250,10000),PlannerTest.OPERATOR).count());
                Files.createDirectories(Path.of("target/step2-evidence"));
                json.writerWithDefaultPrettyPrinter().writeValue(Path.of("target/step2-evidence/verifier-input.json").toFile(),stored);
            }
        } finally {broker.destroy();}
    }
    static byte[] bytes(JsonNode value) { return value.isNull()?null:Base64.getDecoder().decode(value.asText()); }
    static void seed(KafkaProducer<byte[],byte[]> producer,JsonNode row) throws Exception {
        var record=new ProducerRecord<byte[],byte[]>(row.get("topic").asText(),row.get("partition").asInt(),row.get("timestamp").asLong(),bytes(row.get("keyBase64")),bytes(row.get("valueBase64")));
        for(var h:row.get("headers"))record.headers().add(h.get("name").asText(),bytes(h.get("base64")));
        var result=producer.send(record).get(10,TimeUnit.SECONDS);assertEquals(row.get("offset").asLong(),result.offset());
    }
}
