package dev.replay;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

@Service
class WorkerMetrics {
    private final WorkerStore store;
    WorkerMetrics(WorkerStore store) {this.store=store;}
    Map<String,Object> dashboard(String id,Planner.Actor actor) {
        var job=store.planner.job(id,actor);var execution=store.execution(id);
        var data=new LinkedHashMap<String,Object>();long time=store.now();
        data.put("job",job);data.put("observedAt",time);
        data.put("source",store.profiles.live?"Committed Kafka receipts; PostgreSQL projection":"Synthetic committed receipts; local projection");
        data.put("started",execution!=null&&execution.startedAt()>0);
        data.put("published",execution==null?0:execution.published());data.put("skipped",execution==null?0:execution.skipped());
        data.put("processed",execution==null?0:execution.nextPosition());data.put("batches",execution==null?0:execution.batchCount());
        data.put("statusDetail",execution==null?null:execution.detail());
        data.put("progressObservedAt",execution==null?null:execution.lastReceiptAt());
        data.put("recoveryBoundary",execution==null?null:execution.recoveryBoundary());
        var plan=store.plan(id);
        double age=plan.preview().entries().stream().mapToLong(e->Math.max(0,time-e.sourceTimestamp())).average().orElse(0);
        data.put("meanRecordAgeMs",age);data.put("ageSource","Approved source timestamps; age sampled at observedAt");
        long approved=store.planner.db.queryForObject("SELECT approved_at FROM replay_jobs WHERE id=?",(r,n)->r.getTimestamp(1).getTime(),id);
        data.put("timeToFirstReplayMs",execution==null||execution.firstPublishedAt()==null?null:Math.max(0,execution.firstPublishedAt()-approved));
        data.put("publishRatePerSecond",execution==null||execution.firstPublishedAt()==null?0:
                execution.published()*1000.0/Math.max(1000,(execution.lastReceiptAt()==null?time:execution.lastReceiptAt())-execution.firstPublishedAt()));
        data.put("rateSource","Committed published outcomes / receipt timestamp interval (minimum one second)");
        var links=new ArrayList<Map<String,Object>>();
        for(var receipt:store.receipts(id)) {
            var link=new LinkedHashMap<String,Object>();link.put("receiptTopic",execution==null||execution.anchor()==null?null:execution.anchor().topic());
            link.put("receiptOffset",receipt.receiptOffset());link.put("hash",receipt.hash());link.put("receipt",receipt.receipt());links.add(link);
        }
        data.put("receipts",links);
        data.put("events",store.planner.db.queryForList("SELECT event_type,actor,at FROM replay_events WHERE job_id=? ORDER BY id",id));
        data.put("commands",store.planner.db.queryForList("SELECT at,actor,action FROM browse_audit WHERE coordinates=? ORDER BY id",id));
        data.put("groupLag",groupLag(job,time));
        return data;
    }
    Map<String,Object> groupLag(Planner.Job job,long time) {
        String group=store.profiles.env.getProperty("replay.metrics-group","");
        if(!store.profiles.live||group.isBlank())return Map.of("available",false,"reason","No live metrics group configured","observedAt",time,"source","Not sampled");
        var properties=store.profiles.props(store.profiles.credentials(job.mapping().profileId()));
        try(Admin admin=Admin.create(properties)) {
            var offsets=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
            var partitions=offsets.keySet().stream().filter(p->p.topic().equals(job.mapping().target())).toList();
            if(partitions.isEmpty())return Map.of("available",false,"reason","Group has no committed destination offsets","observedAt",time,"source","Kafka Admin group offsets");
            properties.put("key.deserializer",ByteArrayDeserializer.class);properties.put("value.deserializer",ByteArrayDeserializer.class);
            properties.put("enable.auto.commit","false");properties.put("isolation.level","read_committed");properties.put("allow.auto.create.topics","false");
            try(var consumer=new KafkaConsumer<byte[],byte[]>(properties)) {
                var ends=consumer.endOffsets(partitions);var lag=new TreeMap<Integer,Long>();
                for(TopicPartition tp:partitions)lag.put(tp.partition(),Math.max(0,ends.get(tp)-offsets.get(tp).offset()));
                return Map.of("available",true,"group",group,"partitions",lag,"observedAt",System.currentTimeMillis(),"source","Kafka committed group offsets vs read_committed destination end offsets");
            }
        } catch(Exception e) {return Map.of("available",false,"reason","Group metrics unavailable; verify permissions/connectivity","observedAt",time,"source","Kafka Admin sample failed");}
    }
}
