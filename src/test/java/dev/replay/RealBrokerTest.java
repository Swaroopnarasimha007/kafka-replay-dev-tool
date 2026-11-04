package dev.replay;
import java.util.*;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.*;
class RealBrokerTest {
 @Test void browseDoesNotChangeGroupOffsetsAndProfilesSurviveRecreation()throws Exception {
  var broker=new EmbeddedKafkaKraftBroker(1,3,"orders.DLT");broker.afterPropertiesSet();
  try {
   var env=new MockEnvironment().withProperty("replay.encryption-key",Base64.getEncoder().encodeToString(new byte[32])).withProperty("replay.allowed-hosts","localhost,127.0.0.1").withProperty("replay.allow-plaintext","true").withProperty("replay.topic-pattern",".*DLT");env.setActiveProfiles("live");
   var db=new JdbcTemplate(new DriverManagerDataSource("jdbc:h2:mem:broker;MODE=PostgreSQL;DB_CLOSE_DELAY=-1","sa",""));var beans=new StaticListableBeanFactory();beans.addBean("jdbc",db);
   var profiles=new Profiles(env,beans.getBeanProvider(JdbcTemplate.class),new ObjectMapper());
   var in=new Profiles.Input("Real local fixture",broker.getBrokersAsString(),"PLAINTEXT",null,null,null,null,null);
   var tested=profiles.test(in);var profile=profiles.save((String)tested.get("testToken"));
   var config=profiles.props(in);config.put("key.serializer",ByteArraySerializer.class);config.put("value.serializer",ByteArraySerializer.class);
   try(var producer=new KafkaProducer<byte[],byte[]>(config)) {for(int p=0;p<3;p++)for(int i=0;i<20;i++){var r=new ProducerRecord<byte[],byte[]>("orders.DLT",p,(long)i*1000,new byte[]{(byte)i},i==4?null:new byte[]{0,(byte)255,(byte)i});r.headers().add("trace",new byte[]{1}).add("trace",new byte[]{2}).add("kafka_dlt-exception-fqcn","FixtureFailure".getBytes(StandardCharsets.UTF_8));producer.send(r).get();}}
   try(Admin admin=Admin.create(profiles.props(in))) {
    var tp=new TopicPartition("orders.DLT",0);admin.alterConsumerGroupOffsets("existing-application",Map.of(tp,new org.apache.kafka.clients.consumer.OffsetAndMetadata(7))).all().get();var before=admin.listConsumerGroupOffsets("existing-application").partitionsToOffsetAndMetadata().get();
    var restarted=new Profiles(env,beans.getBeanProvider(JdbcTemplate.class),new ObjectMapper());var browser=new Browser(restarted);assertEquals(in,restarted.credentials(profile.id()));assertEquals(List.of("orders.DLT"),browser.topics(profile.id()));
    Set<String> seen=new HashSet<>();Map<Integer,Long> cursors=null;boolean more;do{var page=browser.browse(profile.id(),"orders.DLT",new Browser.Query(null,null,null,null,"FixtureFailure","trace",cursors,7,9),true);for(var r:page.records()){assertTrue(seen.add(r.partition()+":"+r.offset()));assertEquals(2,r.headers().stream().filter(h->h.name().equals("trace")).count());if(r.offset()==4)assertNull(r.valueBase64());}cursors=page.cursors();more=page.truncated();}while(more);assertEquals(60,seen.size());
    assertEquals(before,admin.listConsumerGroupOffsets("existing-application").partitionsToOffsetAndMetadata().get());
    var filtered=browser.browse(profile.id(),"orders.DLT",new Browser.Query(0,5000L,9000L,"Bw==",null,null,null,10,20),true);assertEquals(1,filtered.records().size());assertEquals(7,filtered.records().get(0).offset());
   }
  } finally {broker.destroy();}
 }
}
