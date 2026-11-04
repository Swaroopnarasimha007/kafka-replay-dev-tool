package dev.kafkalab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Two finite producers and two consumer-group members against an actual Kafka broker. */
public final class Main {
    static final ObjectMapper JSON=new ObjectMapper();
    static final Set<String> CODES=Set.of("NONE","VALIDATION","BUSINESS_REJECT","TRANSIENT","INVALID_JSON");
    static final AtomicBoolean RUNNING=new AtomicBoolean(true);
    static volatile Properties settings;
    static Path config;
    static long modified;
    static String boot=UUID.randomUUID().toString();
    static String get(Properties p,String key,String fallback){return p.getProperty(key,fallback).trim();}
    static int number(Properties p,String key,int fallback){return Integer.parseInt(get(p,key,""+fallback));}
    static boolean flag(Properties p,String key){return Boolean.parseBoolean(get(p,key,key.endsWith("seed.dlt")?"false":"true"));}
    static Set<String> list(Properties p,String key){var result=new HashSet<String>();for(String v:get(p,key,"").split(","))if(!v.isBlank())result.add(v.trim());return result;}
    static String mainTopic(Properties p){return get(p,"topic.main","orders");}
    static String dlt(Properties p){return get(p,"topic.dlt","orders.DLT");}
    static Properties client(Properties p){var c=new Properties();c.put("bootstrap.servers",System.getenv().getOrDefault("KAFKA_BOOTSTRAP",get(p,"bootstrap.servers","localhost:19092")));c.put("request.timeout.ms","10000");c.put("default.api.timeout.ms","15000");return c;}
    static Properties producerConfig(Properties p,String id){var c=client(p);c.put("key.serializer",ByteArraySerializer.class);c.put("value.serializer",ByteArraySerializer.class);c.put("acks","all");c.put("enable.idempotence","true");c.put("max.block.ms","15000");c.put("delivery.timeout.ms","30000");c.put("max.request.size","4194304");c.put("client.id",id);return c;}
    static synchronized void event(String kind,Map<String,?> fields){try{var e=new LinkedHashMap<String,Object>();e.put("at",System.currentTimeMillis());e.put("event",kind);e.putAll(fields);String line=JSON.writeValueAsString(e);System.out.println(line);Files.createDirectories(Path.of("data"));Files.writeString(Path.of("data/events.jsonl"),line+System.lineSeparator(),StandardOpenOption.CREATE,StandardOpenOption.APPEND);}catch(Exception e){throw new IllegalStateException(e);}}
    static void validate(Properties p){
        for(String k:List.of("topic.main","topic.dlt"))if(!get(p,k,"").matches("[a-zA-Z0-9._-]{1,249}"))throw new IllegalArgumentException("Invalid "+k);
        if(mainTopic(p).equals(dlt(p))||List.of(mainTopic(p),dlt(p)).contains("_replay_console_receipts"))throw new IllegalArgumentException("Topics must differ from each other and receipts");
        var forbidden=list(p,"errors.forbidden");if(!CODES.containsAll(forbidden))throw new IllegalArgumentException("Unknown forbidden error");
        for(int i=1;i<=2;i++){
            for(String phase:List.of("initial","replay")){var errors=list(p,"consumer."+i+"."+phase+".errors");if(!CODES.containsAll(errors)||errors.contains("NONE")||!Collections.disjoint(errors,forbidden))throw new IllegalArgumentException("Invalid/forbidden consumer error list");}
            String base="producer."+i+".";String code=get(p,base+"error","NONE");if(!CODES.contains(code)||forbidden.contains(code))throw new IllegalArgumentException("Unknown/forbidden producer error");
            for(String k:List.of("count","interval.ms","error.every","payload.bytes")){int n=number(p,base+k,0);if(n<0||n>2_000_000)throw new IllegalArgumentException("Invalid "+base+k);}
            if(number(p,base+"payload.bytes",128)>1_500_000)throw new IllegalArgumentException("Payload maximum is 1500000");
            if(!Double.isFinite(Double.parseDouble(get(p,base+"amount","100"))))throw new IllegalArgumentException("Amount must be finite");
            double minimum=Double.parseDouble(get(p,"consumer."+i+".minimum.amount","0"));if(!Double.isFinite(minimum)||minimum<0)throw new IllegalArgumentException("Minimum amount must be finite and nonnegative");
            if(number(p,"consumer."+i+".delay.ms",0)<0||number(p,"consumer."+i+".delay.ms",0)>30000)throw new IllegalArgumentException("Consumer delay 0..30000");
            for(String k:List.of(base+"enabled",base+"seed.dlt","consumer."+i+".enabled"))if(!Set.of("true","false").contains(get(p,k,"false")))throw new IllegalArgumentException("Boolean required: "+k);
        }
        if(number(p,"consumer.max.dlt.hops",3)<1)throw new IllegalArgumentException("Hop limit must be positive");
        for(String t:List.of("main","dlt")){if(number(p,"topic."+t+".max.message.bytes",1048588)<512)throw new IllegalArgumentException("Topic max bytes must be >=512");long n=Long.parseLong(get(p,"topic."+t+".retention.ms","-1"));if(n < -1)throw new IllegalArgumentException("Retention must be -1 or nonnegative");}
        if(settings!=null)for(String key:List.of("topic.main","topic.dlt","consumer.group","bootstrap.servers"))if(!Objects.equals(p.getProperty(key),settings.getProperty(key)))throw new IllegalArgumentException(key+" requires restart");
    }
    static void configure(Properties p)throws Exception{
        try(Admin a=Admin.create(client(p))){
            for(var topic:List.of(new NewTopic(mainTopic(p),3,(short)1),new NewTopic(dlt(p),3,(short)1),new NewTopic("_replay_console_receipts",1,(short)1))){try{a.createTopics(List.of(topic)).all().get();}catch(ExecutionException e){if(!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException))throw e;}}
            Map<String,TopicDescription> descriptions=null;
            for(int attempt=0;attempt<40;attempt++){
                try{descriptions=a.describeTopics(List.of(mainTopic(p),dlt(p),"_replay_console_receipts")).allTopicNames().get();break;}
                catch(ExecutionException e){if(!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)||attempt==39)throw e;Thread.sleep(250);}
            }
            for(String name:List.of(mainTopic(p),dlt(p)))if(descriptions.get(name).partitions().size()!=3)throw new IllegalArgumentException(name+" must have 3 partitions");
            if(descriptions.get("_replay_console_receipts").partitions().size()!=1)throw new IllegalArgumentException("Receipt topic must have one partition");
            var changes=new HashMap<ConfigResource,Collection<AlterConfigOp>>();
            for(String kind:List.of("main","dlt")){var ops=new ArrayList<AlterConfigOp>();for(String attr:List.of("max.message.bytes","retention.ms"))ops.add(new AlterConfigOp(new ConfigEntry(attr,get(p,"topic."+kind+"."+attr,attr.equals("retention.ms")?"-1":"1048588")),AlterConfigOp.OpType.SET));changes.put(new ConfigResource(ConfigResource.Type.TOPIC,kind.equals("main")?mainTopic(p):dlt(p)),ops);}
            changes.put(new ConfigResource(ConfigResource.Type.TOPIC,"_replay_console_receipts"),List.of(new AlterConfigOp(new ConfigEntry("cleanup.policy","delete"),AlterConfigOp.OpType.SET),new AlterConfigOp(new ConfigEntry("retention.ms","-1"),AlterConfigOp.OpType.SET),new AlterConfigOp(new ConfigEntry("retention.bytes","-1"),AlterConfigOp.OpType.SET)));
            a.incrementalAlterConfigs(changes).all().get();
            var info=new LinkedHashMap<String,Object>();info.put("bootstrap",client(p).getProperty("bootstrap.servers"));info.put("protocol","PLAINTEXT");info.put("clusterId",a.describeCluster().clusterId().get());info.put("source",dlt(p));info.put("target",mainTopic(p));info.put("partitions",3);info.put("consumerGroup",get(p,"consumer.group","orders-demo"));info.put("brokers",a.describeCluster().nodes().get().stream().map(n->n.host()+":"+n.port()).toList());Files.createDirectories(Path.of("data"));Files.writeString(Path.of("data/connection.json"),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(info));
        }
    }
    static void reload()throws Exception{long stamp=Files.getLastModifiedTime(config).toMillis();if(settings!=null&&stamp==modified)return;var p=new Properties();try(var in=Files.newInputStream(config)){p.load(in);}validate(p);configure(p);settings=p;modified=stamp;event("CONFIG_APPLIED",Map.of("file",config.toString()));}
    static RecordHeaders headers(String producer){var h=new RecordHeaders();h.add("demo-producer",producer.getBytes(StandardCharsets.UTF_8));return h;}
    static RecordHeaders deadHeaders(org.apache.kafka.common.header.Headers input,int partition,long offset,int hops,String code,Properties p){var h=new RecordHeaders(input.toArray());for(String k:List.of("demo-dlt-hops","kafka_dlt-original-topic","kafka_dlt-original-partition","kafka_dlt-original-offset","kafka_dlt-exception-fqcn"))h.remove(k);h.add("demo-dlt-hops",ByteBuffer.allocate(4).putInt(hops).array());h.add("kafka_dlt-original-topic",mainTopic(p).getBytes(StandardCharsets.UTF_8));h.add("kafka_dlt-original-partition",ByteBuffer.allocate(4).putInt(partition).array());h.add("kafka_dlt-original-offset",ByteBuffer.allocate(8).putLong(offset).array());h.add("kafka_dlt-exception-fqcn",("dev.kafkalab."+code).getBytes(StandardCharsets.UTF_8));return h;}
    static void produce(int id){String seen="";String prefix="producer."+id+".";try(var producer=new KafkaProducer<byte[],byte[]>(producerConfig(settings,"demo-producer-"+id))){while(RUNNING.get()){
        var p=settings;String run=get(p,"producer.run.id","run");if(!flag(p,prefix+"enabled")||run.equals(seen)){Thread.sleep(250);continue;}seen=run;
        int count=number(p,prefix+"count",30);for(int n=0;n<count&&RUNNING.get();n++){
            if(!flag(settings,prefix+"enabled"))break;
            int every=number(p,prefix+"error.every",0);String code=every>0&&(n+1)%every==0?get(p,prefix+"error","NONE"):"NONE";
            String key=boot+"/"+run+"/p"+id+"/"+n;int partition=n%3;var h=headers(""+id);
            byte[] value=code.equals("INVALID_JSON")?("{invalid:"+key).getBytes(StandardCharsets.UTF_8):JSON.writeValueAsBytes(Map.of("id",key,"producer",id,"error",code,"customer",get(p,prefix+"customer","test"),"amount",Double.parseDouble(get(p,prefix+"amount","100")),"padding","x".repeat(number(p,prefix+"payload.bytes",128))));
            String topic=flag(p,prefix+"seed.dlt")?dlt(p):mainTopic(p);if(topic.equals(dlt(p)))h=deadHeaders(h,partition,-1,1,code,p);
            try{var result=producer.send(new ProducerRecord<byte[],byte[]>(topic,partition,key.getBytes(StandardCharsets.UTF_8),value,h)).get();event("PRODUCED",Map.of("producer",id,"topic",topic,"partition",partition,"offset",result.offset(),"code",code));}catch(Exception e){event("PRODUCER_ERROR",Map.of("producer",id,"error",root(e)));}
            Thread.sleep(number(p,prefix+"interval.ms",100));
        }
    }}catch(InterruptedException e){Thread.currentThread().interrupt();}catch(Exception e){event("PRODUCER_STOPPED",Map.of("producer",id,"error",root(e)));}}
    static String root(Throwable e){while(e.getCause()!=null)e=e.getCause();return e.getClass().getSimpleName();}
    static void consume(int id){var c=client(settings);c.put("group.id",get(settings,"consumer.group","orders-demo"));c.put("client.id","demo-consumer-"+id);c.put("key.deserializer",ByteArrayDeserializer.class);c.put("value.deserializer",ByteArrayDeserializer.class);c.put("auto.offset.reset","earliest");c.put("enable.auto.commit","false");c.put("isolation.level","read_committed");c.put("max.poll.records","1");var pc=producerConfig(settings,"demo-dlt-"+id);pc.put("transactional.id","kafka-lab-consumer-"+id);
        try(var consumer=new KafkaConsumer<byte[],byte[]>(c);var producer=new KafkaProducer<byte[],byte[]>(pc)){
            producer.initTransactions();consumer.subscribe(List.of(mainTopic(settings)));
            while(RUNNING.get()){
                if(!flag(settings,"consumer."+id+".enabled"))consumer.pause(consumer.assignment());else consumer.resume(consumer.assignment());
                var records=consumer.poll(Duration.ofMillis(250));
                for(var r:records){if(!flag(settings,"consumer."+id+".enabled")){consumer.seek(new TopicPartition(r.topic(),r.partition()),r.offset());continue;}var p=settings;var hop=r.headers().lastHeader("demo-dlt-hops");int hops=hop==null?0:ByteBuffer.wrap(hop.value()).getInt();boolean replay=hops>0;String code="NONE";
                    Exception failure=null;
                    var enabled=list(p,"consumer."+id+"."+(replay?"replay":"initial")+".errors");
                    try{var value=JSON.readTree(r.value());code=value.path("error").asText("NONE");
                        if(code.equals("NONE")&&value.path("amount").asDouble(0)<Double.parseDouble(get(p,"consumer."+id+".minimum.amount","0")))code="VALIDATION";
                        if(code.equals("NONE")&&list(p,"consumer."+id+".rejected.customers").contains(value.path("customer").asText()))code="BUSINESS_REJECT";
                        if(enabled.contains(code))switch(code){
                            case "VALIDATION" -> throw new IllegalArgumentException("Order failed validation");
                            case "BUSINESS_REJECT" -> throw new IllegalStateException("Order rejected by consumer policy");
                            case "TRANSIENT" -> throw new java.util.concurrent.TimeoutException("Injected downstream timeout");
                        }
                    }catch(com.fasterxml.jackson.core.JsonProcessingException e){
                        code="INVALID_JSON";if(enabled.contains(code))failure=e;
                    }catch(IllegalArgumentException | IllegalStateException | java.util.concurrent.TimeoutException e){failure=e;}
                    boolean fail=failure!=null;
                    if(fail)event("CONSUMER_EXCEPTION",Map.of("consumer",id,"exception",failure.getClass().getName(),"offset",r.offset(),"partition",r.partition()));
                    if(fail&&hops>=number(p,"consumer.max.dlt.hops",3)){consumer.seek(new TopicPartition(r.topic(),r.partition()),r.offset());event("HOP_LIMIT_HELD",Map.of("consumer",id,"partition",r.partition(),"offset",r.offset()));Thread.sleep(1000);continue;}
                    Thread.sleep(number(p,"consumer."+id+".delay.ms",0));
                    producer.beginTransaction();
                    try{
                        if(fail){
                            var headers=deadHeaders(r.headers(),r.partition(),r.offset(),hops+1,code,p);
                            headers.remove("kafka_dlt-exception-fqcn");
                            headers.add("kafka_dlt-exception-fqcn",failure.getClass().getName().getBytes(StandardCharsets.UTF_8));
                            producer.send(new ProducerRecord<byte[],byte[]>(dlt(p),r.partition(),r.key(),r.value(),headers)).get();
                        }
                        producer.sendOffsetsToTransaction(Map.of(new TopicPartition(r.topic(),r.partition()),new OffsetAndMetadata(r.offset()+1)),consumer.groupMetadata());producer.commitTransaction();
                        event(fail?"DEAD_LETTERED":"PROCESSED",Map.of("consumer",id,"partition",r.partition(),"offset",r.offset(),"replay",replay,"code",code));
                    }catch(Exception e){try{producer.abortTransaction();}catch(Exception ignored){}throw e;}
                }
            }
        }catch(Exception e){event("CONSUMER_STOPPED",Map.of("consumer",id,"error",root(e)));RUNNING.set(false);}
    }
    public static void main(String[] args)throws Exception{
        if(args.length==0)throw new IllegalArgumentException("Use broker <properties>, format <cluster-id> <properties>, run <demo.properties>, or inspect <demo.properties>");
        if(args[0].equals("broker")){Class.forName("kafka.Kafka").getMethod("main",String[].class).invoke(null,(Object)new String[]{args[1]});return;}
        if(args[0].equals("format")){Class.forName("kafka.tools.StorageTool").getMethod("main",String[].class).invoke(null,(Object)new String[]{"format","--cluster-id="+args[1],"-c",args[2],"--ignore-formatted"});return;}
        config=Path.of(args.length>1?args[1]:"config/demo.properties");reload();
        if(args[0].equals("inspect")){System.out.println(Files.readString(Path.of("data/connection.json")));return;}
        if(!args[0].equals("run"))throw new IllegalArgumentException("Unknown command");
        var pool=Executors.newFixedThreadPool(4);Runtime.getRuntime().addShutdownHook(new Thread(()->{RUNNING.set(false);pool.shutdown();try{pool.awaitTermination(10,TimeUnit.SECONDS);}catch(InterruptedException ignored){}}));
        for(int i=1;i<=2;i++){final int id=i;pool.submit(()->consume(id));pool.submit(()->produce(id));}
        while(RUNNING.get()){Thread.sleep(1000);try{reload();}catch(Exception e){event("CONFIG_REJECTED",Map.of("error",root(e),"detail",Objects.toString(e.getMessage(),"invalid configuration")));}}
        pool.shutdown();pool.awaitTermination(15,TimeUnit.SECONDS);System.exit(1);
    }
}
