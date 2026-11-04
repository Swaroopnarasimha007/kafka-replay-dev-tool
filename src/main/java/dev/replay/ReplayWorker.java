package dev.replay;

import java.util.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
class ReplayWorker {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(ReplayWorker.class);
    private final WorkerStore store;
    private final ReplayTransport transport;
    ReplayWorker(WorkerStore store,ReplayTransport transport) {this.store=store;this.transport=transport;}

    @Scheduled(fixedDelayString="${replay.worker-tick-ms:1000}")
    void tick() {
        if(!store.enabled()||!store.profiles.env.getProperty("replay.scheduler-enabled",Boolean.class,true))return;
        for(var id:store.runnable().stream().limit(20).toList()) {
            try {runOne(id,ReplayTransport.Hook.NONE);}
            catch(RuntimeException ignored) { /* runOne persists a redacted blocked reason. Retry is explicit. */ }
        }
    }
    boolean runOne(String id,ReplayTransport.Hook hook) {
        if(!store.enabled())return false;
        String token=store.claim(id);if(token==null)return false;
        boolean release=true;
        try {
            var plan=store.plan(id);var e=store.execution(id);store.authorize(e,plan);
            if(!plan.job().digest().equals(Manifest.digest(plan.job().mapping(),plan.job().rate(),plan.preview().entries()))
                    ||!plan.preview().digest().equals(plan.job().digest())||plan.job().count()!=plan.preview().entries().size())
                throw new ReplayTransport.Blocked("MANIFEST_INTEGRITY_FAILURE");
            try(var session=transport.open(plan)) {
                store.planner.transaction(()->{store.requireLease(id,token);return null;});
                session.initialize();
                var anchor=e.anchor();
                if(anchor==null){anchor=session.newAnchor();store.anchor(id,token,anchor);}
                var history=session.history(anchor);validateHistory(plan,history);
                store.planner.transaction(()->{
                    store.requireLease(id,token);store.planner.lockMapping(plan.job().mapping().id());
                    store.authorize(store.execution(id),plan);store.project(id,token,plan,history);return null;
                });
                e=store.execution(id);
                if(!e.requested()||e.nextEligible()>store.now())return true;
                int size=store.profiles.env.getProperty("replay.batch-size",Integer.class,50);
                if(size<1||size>500)throw new ReplayTransport.Blocked("INVALID_BATCH_SIZE: choose 1..500");
                int count=plan.job().canary()&&!e.released()?1:Math.min(size,plan.job().rate());
                int end=Math.min(plan.job().count(),e.nextPosition()+count);
                var entries=plan.preview().entries().subList(e.nextPosition(),end);
                var validated=session.validate(entries);
                if(validated.size()!=entries.size())throw new ReplayTransport.Blocked("INVALID_SOURCE_VALIDATION");
                int from=e.nextPosition();
                var previous=history.receipts().isEmpty()?"GENESIS":history.receipts().get(history.receipts().size()-1).hash();
                var batch=new ReplayTransport.Batch(plan,history.receipts().size(),from,previous,validated);
                store.planner.transaction(()->{
                    var current=store.requireLease(id,token);store.planner.lockMapping(plan.job().mapping().id());
                    store.authorize(current,plan);
                    if(current.pauseRequested()){store.project(id,token,plan,history);return null;}
                    // Keep the execution/mapping rows locked through commit. New owners fence the stable
                    // producer before recovering; database rollback cannot undo a Kafka commit.
                    var committed=session.publish(batch,hook);
                    var all=new ArrayList<>(history.receipts());all.add(committed);
                    var updated=new ReplayTransport.History(committed.receiptOffset()+1,List.copyOf(all));
                    validateHistory(plan,updated);store.project(id,token,plan,updated);return null;
                });
            }
            return true;
        } catch(WorkerStore.LeaseLost lost) {return false;}
        catch(ReplayTransport.Blocked blocked) {store.block(id,token,blocked.getMessage());return false;}
        catch(org.springframework.security.access.AccessDeniedException denied) {store.block(id,token,"MAPPING_AUTHORIZATION_REVOKED");return false;}
        catch(RuntimeException failure) {
            LOG.warn("Replay job {} blocked by {} (details suppressed)",id,failure.getClass().getSimpleName());
            store.block(id,token,"WORKER_ERROR: reconcile receipts and verify infrastructure before resuming");return false;
        }
        catch(Error simulatedCrash) {release=false;throw simulatedCrash;}
        finally {if(release)store.releaseLease(id,token);}
    }
    static void validateHistory(Planner.StoredJob plan,ReplayTransport.History history) {
        int next=0,index=0;String previous="GENESIS";long offset=-1;
        var destinations=new HashSet<String>();
        for(var committed:history.receipts()) {
            var receipt=committed.receipt();
            if(receipt.version()!=1||!receipt.jobId().equals(plan.job().id())||!receipt.manifestDigest().equals(plan.job().digest())
                    ||!receipt.targetTopicId().equals(plan.job().mapping().identity().targetId())||receipt.batchIndex()!=index++
                    ||receipt.fromPosition()!=next||!receipt.previousHash().equals(previous)||committed.receiptOffset()<=offset
                    ||receipt.nextPosition()<=next||receipt.nextPosition()>plan.job().count()
                    ||receipt.outcomes().size()!=receipt.nextPosition()-next)
                throw new ReplayTransport.Blocked("INVALID_OR_INCOMPLETE_RECEIPT_CHAIN");
            if(plan.job().canary()&&next==0&&receipt.outcomes().size()!=1)throw new ReplayTransport.Blocked("INVALID_CANARY_RECEIPT");
            for(var outcome:receipt.outcomes()) {
                var entry=plan.preview().entries().get(next);
                if(outcome.position()!=next||!outcome.clusterId().equals(entry.clusterId())||!outcome.sourceTopicId().equals(entry.topicId())
                        ||!outcome.sourceTopic().equals(entry.topic())||outcome.sourcePartition()!=entry.partition()||outcome.sourceOffset()!=entry.offset()
                        ||!outcome.fingerprint().equals(entry.fingerprint()))throw new ReplayTransport.Blocked("RECEIPT_MANIFEST_MISMATCH");
                if(outcome.status().equals("PUBLISHED")) {
                    if(!plan.job().mapping().target().equals(outcome.destinationTopic())||!Objects.equals(entry.destinationPartition(),outcome.destinationPartition())
                            ||outcome.destinationOffset()==null||outcome.destinationOffset()<0||outcome.destinationTimestamp()==null
                            ||!destinations.add(outcome.destinationPartition()+":"+outcome.destinationOffset()))
                        throw new ReplayTransport.Blocked("INVALID_DESTINATION_RECEIPT");
                } else if(!Set.of("SKIPPED_EXPIRED","SKIPPED_GAP").contains(outcome.status())||outcome.destinationOffset()!=null
                        ||outcome.destinationTopic()!=null||outcome.destinationPartition()!=null||outcome.destinationTimestamp()!=null)
                    throw new ReplayTransport.Blocked("INVALID_SKIPPED_OUTCOME");
                next++;
            }
            offset=committed.receiptOffset();previous=committed.hash();
        }
    }
}
