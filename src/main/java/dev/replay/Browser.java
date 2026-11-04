package dev.replay;
import java.util.*;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import org.springframework.stereotype.Service;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
@Service
class Browser {
 record Header(String name,String base64) {}
 record Row(String topic,int partition,long offset,long timestamp,String keyBase64,String valueBase64,List<Header> headers,boolean masked) {}
 record Query(Integer partition,Long from,Long to,String key,String exception,String header,Map<Integer,Long> cursors,Integer limit,Integer scanLimit) {}
 record Page(List<Row> records,Map<Integer,Long> cursors,Map<Integer,Long> beginnings,Map<Integer,Long> boundaries,int scanned,boolean truncated) {}
 final Profiles profiles;
 Browser(Profiles p){profiles=p;}
 List<String> topics(String id) {
  if(!profiles.real(id))return List.of("orders.DLT");
  try(Admin a=Admin.create(profiles.props(profiles.credentials(id)))) {return a.listTopics().names().get(7,java.util.concurrent.TimeUnit.SECONDS).stream().filter(this::approved).sorted().toList();}catch(Exception e){throw Profiles.failure(e);}
 }
 boolean approved(String topic){return topic.matches(profiles.env.getProperty("replay.topic-pattern"))||Arrays.asList(profiles.env.getProperty("replay.topics","").split(",")).contains(topic);}
 static String b64(byte[] bytes){return bytes==null?null:Base64.getEncoder().encodeToString(bytes);}
 static Row row(ConsumerRecord<byte[],byte[]> r,boolean reveal){var headers=new ArrayList<Header>();r.headers().forEach(h->headers.add(new Header(h.key(),b64(h.value()))));return new Row(r.topic(),r.partition(),r.offset(),r.timestamp(),reveal?b64(r.key()):r.key()==null?null:"[masked]",reveal?b64(r.value()):r.value()==null?null:"[masked]",reveal?headers:headers.stream().map(h->new Header(h.name(),h.base64()==null?null:"[masked]")).toList(),!reveal);}
 boolean matches(ConsumerRecord<byte[],byte[]> r,Query q){
  if(q.from()!=null&&r.timestamp()<q.from()||q.to()!=null&&r.timestamp()>q.to())return false;
  if(q.key()!=null&&!q.key().isEmpty()&&!q.key().equals(b64(r.key())))return false;
  if(q.exception()!=null&&!q.exception().isEmpty()){boolean found=false;for(var h:r.headers())if(h.key().contains("exception")&&h.value()!=null&&new String(h.value(),StandardCharsets.UTF_8).contains(q.exception()))found=true;if(!found)return false;}
  if(q.header()!=null&&!q.header().isEmpty()&&r.headers().lastHeader(q.header())==null)return false;
  return true;
 }
 Page browse(String id,String topic,Query q,boolean reveal) {
  if(!approved(topic))throw new IllegalArgumentException("Topic is outside discovery policy");
  int limit=q.limit()==null?50:q.limit(), budget=q.scanLimit()==null?1000:q.scanLimit();
  if(limit<1||limit>200||budget<1||budget>10000)throw new IllegalArgumentException("Limit 1..200; scanLimit 1..10000");
  if(!profiles.real(id))return demo(topic,q,reveal,limit,budget);
  Properties p=profiles.props(profiles.credentials(id));p.put("key.deserializer",ByteArrayDeserializer.class);p.put("value.deserializer",ByteArrayDeserializer.class);p.put("enable.auto.commit","false");p.put("isolation.level","read_committed");p.put("max.poll.records","200");p.put("allow.auto.create.topics","false");
  try(var c=new KafkaConsumer<byte[],byte[]>(p)) {
   var parts=c.partitionsFor(topic).stream().filter(x->q.partition()==null||x.partition()==q.partition()).map(x->new TopicPartition(topic,x.partition())).sorted(Comparator.comparingInt(TopicPartition::partition)).toList();
   if(parts.isEmpty())throw new IllegalArgumentException("Partition unavailable");
   c.assign(parts);var begin=c.beginningOffsets(parts);var end=c.endOffsets(parts);var cursor=new TreeMap<Integer,Long>();var starts=new TreeMap<Integer,Long>();var ends=new TreeMap<Integer,Long>();
   for(var tp:parts){long start=q.cursors()==null?begin.get(tp):q.cursors().getOrDefault(tp.partition(),begin.get(tp));if(start<begin.get(tp)||start>end.get(tp))throw new IllegalArgumentException("Cursor outside retained boundaries");c.seek(tp,start);cursor.put(tp.partition(),start);starts.put(tp.partition(),begin.get(tp));ends.put(tp.partition(),end.get(tp));}
   var out=new ArrayList<Row>();int scanned=0;long deadline=System.nanoTime()+Duration.ofSeconds(8).toNanos();
   outer:while(System.nanoTime()<deadline&&scanned<budget&&out.size()<limit){
    for(var tp:parts)if(cursor.get(tp.partition())>=end.get(tp))c.pause(List.of(tp));
    if(c.paused().size()==parts.size())break;
    var batch=c.poll(Duration.ofMillis(300));
    for(var r:batch){var tp=new TopicPartition(topic,r.partition());if(r.offset()>=end.get(tp))continue;cursor.put(r.partition(),r.offset()+1);scanned++;if(matches(r,q))out.add(row(r,reveal));if(scanned>=budget||out.size()>=limit)break outer;}
    // Advance over compacted/transaction control gaps only after processing the complete poll.
    for(var tp:parts)cursor.put(tp.partition(),Math.min(c.position(tp),end.get(tp)));
   }
   boolean truncated=parts.stream().anyMatch(tp->cursor.get(tp.partition())<end.get(tp));return new Page(out,cursor,starts,ends,scanned,truncated);
  } catch(IllegalArgumentException e){throw e;}catch(Exception e){throw Profiles.failure(e);}
 }
 Page demo(String topic,Query q,boolean reveal,int limit,int budget){
  var rows=new ArrayList<Row>();var cursors=new TreeMap<Integer,Long>();var beginnings=new TreeMap<Integer,Long>();var ends=new TreeMap<Integer,Long>();int scanned=0;
  for(int p=0;p<3;p++){if(q.partition()!=null&&q.partition()!=p)continue;long offset=q.cursors()==null?0:q.cursors().getOrDefault(p,0L);if(offset<0||offset>20)throw new IllegalArgumentException("Invalid cursor");beginnings.put(p,0L);ends.put(p,20L);
   while(offset<20&&scanned<budget&&rows.size()<limit){var r=new ConsumerRecord<byte[],byte[]>(topic,p,offset,1735689600000L+offset*1000,org.apache.kafka.common.record.TimestampType.CREATE_TIME,-1,-1,offset==5?null:("order-"+offset).getBytes(StandardCharsets.UTF_8),offset==4?null:offset==3?new byte[]{0,(byte)255,1}:("{\"order\":"+offset+",\"email\":\"synthetic@example.test\"}").getBytes(StandardCharsets.UTF_8),new org.apache.kafka.common.header.internals.RecordHeaders(),Optional.empty());r.headers().add("kafka_dlt-original-topic","orders".getBytes(StandardCharsets.UTF_8)).add("kafka_dlt-original-partition",java.nio.ByteBuffer.allocate(4).putInt(p).array()).add("trace","first".getBytes(StandardCharsets.UTF_8)).add("trace","second".getBytes(StandardCharsets.UTF_8)).add("kafka_dlt-exception-fqcn","SyntheticFailure".getBytes(StandardCharsets.UTF_8));offset++;scanned++;if(matches(r,q))rows.add(row(r,reveal));}cursors.put(p,offset);
  }return new Page(rows,cursors,beginnings,ends,scanned,cursors.values().stream().anyMatch(x->x<20));
 }
}

