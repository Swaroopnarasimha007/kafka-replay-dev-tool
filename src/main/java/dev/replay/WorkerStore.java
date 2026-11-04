package dev.replay;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.springframework.stereotype.Service;

@Service
class WorkerStore {
    record Execution(String jobId, boolean requested, Planner.Actor actor, long authorizedUntil,
                     String leaseOwner, long leaseUntil, boolean pauseRequested, boolean released,
                     ReplayTransport.Anchor anchor, int nextPosition, int published, int skipped,
                     int batchCount, long nextEligible, String detail, Long firstPublishedAt,
                     Long lastReceiptAt, Long recoveryBoundary, long startedAt, String resumeState) {}
    static final class LeaseLost extends RuntimeException {}
    final Planner planner;
    final Profiles profiles;

    WorkerStore(Planner planner, Profiles profiles) { this.planner=planner; this.profiles=profiles; }
    long now() { return planner.db.queryForObject("SELECT CURRENT_TIMESTAMP",(r,n)->r.getTimestamp(1).getTime()); }
    long leaseMillis() { return profiles.env.getProperty("replay.lease-ms",Long.class,60000L); }
    boolean enabled() { return !profiles.live || profiles.env.getProperty("replay.worker-enabled",Boolean.class,false); }
    Execution row(ResultSet r) throws SQLException {
        String anchor=r.getString("anchor");
        return new Execution(r.getString("job_id"),r.getBoolean("requested"),planner.decode(r.getString("actor_document"),Planner.Actor.class),
                r.getLong("authorized_until"),r.getString("lease_owner"),r.getLong("lease_until"),r.getBoolean("pause_requested"),r.getBoolean("released"),
                anchor==null?null:planner.decode(anchor,ReplayTransport.Anchor.class),r.getInt("next_position"),r.getInt("published"),r.getInt("skipped"),
                r.getInt("batch_count"),r.getLong("next_eligible"),r.getString("status_detail"),(Long)r.getObject("first_published_at"),
                (Long)r.getObject("last_receipt_at"),(Long)r.getObject("recovery_boundary"),r.getLong("started_at"),r.getString("resume_state"));
    }
    Execution execution(String id) {
        var rows=planner.db.query("SELECT * FROM replay_execution WHERE job_id=?",(r,n)->row(r),id);
        return rows.isEmpty()?null:rows.get(0);
    }
    Execution locked(String id) { return planner.db.queryForObject("SELECT * FROM replay_execution WHERE job_id=? FOR UPDATE",(r,n)->row(r),id); }
    String state(String id) { return planner.db.queryForObject("SELECT state FROM replay_jobs WHERE id=?",String.class,id); }
    void state(String id,String state) { planner.db.update("UPDATE replay_jobs SET state=? WHERE id=?",state,id); }
    void event(String id,String type,String actor) {
        if(planner.db.queryForObject("SELECT count(*) FROM replay_events WHERE job_id=? AND event_type=?",Integer.class,id,type)==0)
            planner.db.update("INSERT INTO replay_events(job_id,event_type,actor) VALUES (?,?,?)",id,type,actor);
    }
    void audit(String id,String action,String actor) {
        planner.db.update("INSERT INTO browse_audit(actor,action,coordinates) VALUES (?,?,?)",actor,action,id);
    }
    Planner.StoredJob plan(String id) {
        var job=planner.db.queryForObject("SELECT document FROM replay_jobs WHERE id=?",(r,n)->planner.decode(r.getString(1),Planner.Job.class),id);
        var preview=planner.db.queryForObject("SELECT document FROM replay_previews WHERE id=?",(r,n)->planner.decode(r.getString(1),Planner.Preview.class),job.previewId());
        return new Planner.StoredJob(job,preview);
    }
    void authorize(Execution execution,Planner.StoredJob plan) {
        if(now()>execution.authorizedUntil()) throw new ReplayTransport.Blocked("AUTHORIZATION_EXPIRED: sign in and resume to reauthorize");
        if(Arrays.asList(profiles.env.getProperty("replay.revoked-subjects","").split(",")).contains(execution.actor().name()))
            throw new ReplayTransport.Blocked("ACTOR_REVOKED");
        var current=planner.mapping(plan.job().mapping().id());
        planner.authorize(execution.actor(),current);
        if(current.version()!=plan.job().mapping().version()) throw new ReplayTransport.Blocked("MAPPING_CHANGED: approved policy requires a new review");
    }
    Planner.Job command(String id,String command,Planner.Actor actor) {
        var job=planner.job(id,actor);
        if(!enabled()) throw new Planner.Conflict("WORKER_DISABLED: enable replay.worker-enabled for live execution");
        if(execution(id)==null) {
            try {
                planner.db.update("INSERT INTO replay_execution(job_id,actor_document,authorized_until,started_at) VALUES (?,?,?,0)",id,planner.encode(actor),now());
            } catch(org.springframework.dao.DuplicateKeyException alreadyCreated) { /* concurrent command initialized it */ }
        }
        return planner.transaction(()->{
            var e=locked(id); String current=state(id); long time=now();
            if(command.equals("start")) {
                if(e.startedAt()>0) return planner.job(id,actor); // A repeated start cannot resume a paused or blocked job.
                if(!current.equals("APPROVED")) throw new Planner.Conflict("Only approved jobs can start");
                if(planner.mapping(job.mapping().id()).version()!=job.mapping().version()) throw new Planner.Conflict("Mapping changed; approve a new manifest");
                long duration=profiles.env.getProperty("replay.authorization-ms",Long.class,900000L);
                planner.db.update("UPDATE replay_execution SET requested=true,actor_document=?,authorized_until=?,started_at=? WHERE job_id=?",
                        planner.encode(actor),time+duration,time,id);
                state(id,job.canary()?"CANARY_RUNNING":"RUNNING");event(id,"STARTED",actor.name());
            } else {
                if(e.startedAt()==0) throw new Planner.Conflict("Start this job first");
                if(command.equals("release")) {
                    if(!e.released()) {
                        if(planner.mapping(job.mapping().id()).version()!=job.mapping().version())throw new Planner.Conflict("Mapping changed; approve a new manifest");
                        if(!current.equals("AWAITING_RELEASE")) throw new Planner.Conflict("Canary must await release");
                        long duration=profiles.env.getProperty("replay.authorization-ms",Long.class,900000L);
                        planner.db.update("UPDATE replay_execution SET released=true,requested=true,pause_requested=false,actor_document=?,authorized_until=? WHERE job_id=?",planner.encode(actor),time+duration,id);
                        state(id,"RUNNING");event(id,"RELEASED",actor.name());
                    }
                } else if(command.equals("pause")) {
                    if(!Set.of("RUNNING","CANARY_RUNNING","PAUSED").contains(current)) throw new Planner.Conflict("This job cannot pause");
                    planner.db.update("UPDATE replay_execution SET pause_requested=true WHERE job_id=?",id);
                } else if(command.equals("resume")) {
                    if(!Set.of("PAUSED","BLOCKED").contains(current)) {
                        if(Set.of("RUNNING","CANARY_RUNNING","AWAITING_RELEASE").contains(current))return planner.job(id,actor);
                        throw new Planner.Conflict("This job cannot resume");
                    }
                    if(planner.mapping(job.mapping().id()).version()!=job.mapping().version())throw new Planner.Conflict("Mapping changed; approve a new manifest");
                    long duration=profiles.env.getProperty("replay.authorization-ms",Long.class,900000L);
                    planner.db.update("UPDATE replay_execution SET requested=true,pause_requested=false,actor_document=?,authorized_until=?,status_detail=null WHERE job_id=?",
                            planner.encode(actor),time+duration,id);
                    state(id,job.canary()&&!e.released()&&e.nextPosition()==0?"CANARY_RUNNING":"RUNNING");
                } else throw new IllegalArgumentException("Unknown worker command");
            }
            audit(id,command,actor.name());
            return planner.job(id,actor);
        });
    }
    List<String> runnable() { return planner.db.queryForList("SELECT e.job_id FROM replay_execution e JOIN replay_jobs j ON e.job_id=j.id WHERE e.requested=true AND j.state IN ('RUNNING','CANARY_RUNNING') ORDER BY e.started_at",String.class); }
    String claim(String id) {
        return planner.transaction(()->{
            var e=locked(id); long time=now();
            if(!e.requested()||e.nextEligible()>time||e.leaseOwner()!=null&&e.leaseUntil()>time)return null;
            if(!Set.of("RUNNING","CANARY_RUNNING").contains(state(id)))return null;
            String token=UUID.randomUUID().toString();
            planner.db.update("UPDATE replay_execution SET lease_owner=?,lease_until=? WHERE job_id=?",token,time+leaseMillis(),id);
            return token;
        });
    }
    Execution requireLease(String id,String token) {
        var e=locked(id);
        if(!token.equals(e.leaseOwner())||e.leaseUntil()<=now())throw new LeaseLost();
        planner.db.update("UPDATE replay_execution SET lease_until=? WHERE job_id=?",now()+leaseMillis(),id);
        return e;
    }
    void anchor(String id,String token,ReplayTransport.Anchor anchor) {
        planner.transaction(()->{requireLease(id,token);planner.db.update("UPDATE replay_execution SET anchor=? WHERE job_id=?",planner.encode(anchor),id);return null;});
    }
    void releaseLease(String id,String token) { planner.db.update("UPDATE replay_execution SET lease_owner=null,lease_until=0 WHERE job_id=? AND lease_owner=?",id,token); }
    void block(String id,String token,String reason) {
        planner.transaction(()->{
            var e=locked(id);
            if(!token.equals(e.leaseOwner()))return null;
            String from=state(id);
            planner.db.update("UPDATE replay_execution SET requested=false,status_detail=?,resume_state=? WHERE job_id=?",reason,from,id);
            state(id,reason.startsWith("MANIFEST_INTEGRITY_FAILURE")?"FAILED":"BLOCKED");audit(id,"blocked:"+reason,e.actor().name());return null;
        });
    }
    List<ReplayTransport.Committed> receipts(String id) {
        return planner.db.query("SELECT document FROM replay_receipt_projection WHERE job_id=? ORDER BY batch_index",(r,n)->planner.decode(r.getString(1),ReplayTransport.Committed.class),id);
    }
    void project(String id,String token,Planner.StoredJob plan,ReplayTransport.History history) {
        var e=requireLease(id,token);var receipts=history.receipts();
        int next=receipts.isEmpty()?0:receipts.get(receipts.size()-1).receipt().nextPosition();
        if(e.nextPosition()>next)throw new ReplayTransport.Blocked("MISSING_PROGRESS_HISTORY: projection is ahead of authoritative receipts");
        int published=0,skipped=0;Long first=null,last=null;
        planner.db.update("DELETE FROM replay_receipt_projection WHERE job_id=?",id);
        for(var committed:receipts) {
            planner.db.update("INSERT INTO replay_receipt_projection(job_id,batch_index,document) VALUES (?,?,?)",id,committed.receipt().batchIndex(),planner.encode(committed));
            for(var outcome:committed.receipt().outcomes())if(outcome.status().equals("PUBLISHED")){published++;if(first==null)first=committed.receipt().committedAt();}else skipped++;
            last=committed.receipt().committedAt();
        }
        String state;
        if(plan.job().canary()&&next>0&&!e.released())state="AWAITING_RELEASE";
        else if(next==plan.job().count())state=skipped==0?"COMPLETED":"COMPLETED_WITH_EXCEPTIONS";
        else if(e.pauseRequested())state="PAUSED";
        else state=plan.job().canary()&&!e.released()?"CANARY_RUNNING":"RUNNING";
        boolean requested=Set.of("CANARY_RUNNING","RUNNING").contains(state);
        long eligible=receipts.isEmpty()?0:receipts.get(receipts.size()-1).receipt().committedAt()
                +(long)Math.ceil(receipts.get(receipts.size()-1).receipt().outcomes().size()*1000.0/plan.job().rate());
        planner.db.update("UPDATE replay_execution SET next_position=?,published=?,skipped=?,batch_count=?,first_published_at=?,last_receipt_at=?,recovery_boundary=?,status_detail=null,requested=? WHERE job_id=?",
                next,published,skipped,receipts.size(),first,last,history.stableBoundary(),requested,id);
        state(id,state);
        planner.db.update("UPDATE replay_execution SET next_eligible=? WHERE job_id=?",eligible,id);
        if(Set.of("AWAITING_RELEASE","COMPLETED","COMPLETED_WITH_EXCEPTIONS").contains(state))event(id,state,e.actor().name());
    }
}
