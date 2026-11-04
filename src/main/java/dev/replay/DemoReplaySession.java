package dev.replay;

import java.util.*;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Synthetic receipt authority; the real worker orchestrator is unchanged. */
final class DemoReplaySession implements ReplayTransport.Session {
    private final Planner.StoredJob plan;
    private final Planner planner;
    private final TransactionTemplate authority;
    DemoReplaySession(Planner.StoredJob plan,Planner planner) {
        this.plan=plan;this.planner=planner;
        authority=new TransactionTemplate(new DataSourceTransactionManager(planner.db.getDataSource()));
        authority.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    public void initialize() {}
    long end() { return planner.db.queryForObject("SELECT coalesce(max(id),0)+1 FROM demo_receipts",Long.class); }
    public ReplayTransport.Anchor newAnchor() { return new ReplayTransport.Anchor("synthetic-receipts","demo-receipts-v1",end()); }
    public ReplayTransport.History history(ReplayTransport.Anchor anchor) {
        if(!anchor.topicId().equals("demo-receipts-v1"))throw new ReplayTransport.Blocked("DEMO_RECEIPT_IDENTITY_CHANGED");
        long boundary=end();
        var rows=planner.db.query("SELECT id,document FROM demo_receipts WHERE job_id=? AND id>=? AND id<? ORDER BY id",(r,n)->{
            String document=r.getString(2);return new ReplayTransport.Committed(planner.decode(document,ReplayTransport.Receipt.class),r.getLong(1),KafkaReplayTransport.hash(document));
        },plan.job().id(),anchor.origin(),boundary);
        return new ReplayTransport.History(boundary,rows);
    }
    public List<ReplayTransport.Validation> validate(List<Manifest.Entry> entries) {
        // Synthetic source bytes are immutable fixture bytes already bound by the approved digest.
        return entries.stream().map(e->new ReplayTransport.Validation(e,"PUBLISH")).toList();
    }
    public ReplayTransport.Committed publish(ReplayTransport.Batch batch,ReplayTransport.Hook hook) {
        var committed=authority.execute(status->{
            var outcomes=new ArrayList<ReplayTransport.Outcome>();
            for(var v:batch.validations()) {
                var e=v.entry();
                planner.db.update("INSERT INTO demo_outputs(job_id,manifest_position,topic,partition_id,document) VALUES (?,?,?,?,?)",plan.job().id(),e.position(),plan.job().mapping().target(),e.destinationPartition(),planner.encode(e));
                long offset=planner.db.queryForObject("SELECT id FROM demo_outputs WHERE job_id=? AND manifest_position=?",Long.class,plan.job().id(),e.position());
                outcomes.add(KafkaReplayTransport.outcome(e,"PUBLISHED",plan.job().mapping().target(),e.destinationPartition(),offset,System.currentTimeMillis()));
                if(batch.fromPosition()==0&&plan.job().canary())hook.at(ReplayTransport.Point.CANARY_ACKNOWLEDGED);
            }
            var receipt=KafkaReplayTransport.receipt(batch,outcomes);String document=planner.encode(receipt);
            planner.db.update("INSERT INTO demo_receipts(job_id,batch_index,document) VALUES (?,?,?)",plan.job().id(),batch.index(),document);
            long offset=planner.db.queryForObject("SELECT id FROM demo_receipts WHERE job_id=? AND batch_index=?",Long.class,plan.job().id(),batch.index());
            hook.at(ReplayTransport.Point.BEFORE_COMMIT);
            return new ReplayTransport.Committed(receipt,offset,KafkaReplayTransport.hash(document));
        });
        hook.at(ReplayTransport.Point.AFTER_COMMIT_BEFORE_PROJECTION);
        return committed;
    }
    public void close() {}
}
