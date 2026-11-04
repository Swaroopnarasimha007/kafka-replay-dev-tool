package dev.replay;

import java.util.*;
import java.util.function.Supplier;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.ResponseStatus;

@Service
class Planner {
    @ResponseStatus(HttpStatus.CONFLICT)
    static class Conflict extends RuntimeException { Conflict(String message) { super(message); } }
    record Actor(String name, Set<String> roles) {
        boolean admin() { return roles.contains("Admin"); }
        void operator() { if (!admin() && !roles.contains("Operator")) throw new AccessDeniedException("Operator role required"); }
    }
    record MappingInput(String profileId, String source, String target, String extractor,
                        String partitionPolicy, String headerPolicy, List<String> removeHeaders,
                        List<String> allowedSubjects, boolean bytePreservingAttested, Integer expectedVersion) {}
    record Mapping(String id, int version, String profileId, String source, String target,
                   String extractor, String partitionPolicy, String headerPolicy, List<String> removeHeaders,
                   List<String> allowedSubjects, boolean bytePreservingAttested, ReplaySource.Identity identity) {}
    record PreviewRequest(String mappingId, Browser.Query filter, int rate, int scanLimit) {}
    record Preview(String id, String owner, Mapping mapping, int rate, ReplaySource.Scan scan,
                   List<Manifest.Entry> entries, String digest) {}
    record Coordinate(int position, int partition, long offset, String fingerprint, int destinationPartition) {}
    record PreviewView(String id, Mapping mapping, int rate, int count, String digest, int scanned,
                       boolean partial, Map<Integer,Long> beginnings, Map<Integer,Long> boundaries,
                       Map<Integer,Long> cursors, List<Coordinate> records, boolean workerEnabled) {}
    record Approval(String previewId, String digest, String idempotencyKey, String reason,
                    Boolean canary, boolean acceptPartial, List<Integer> selectedPositions) {}
    record Job(String id, String owner, String previewId, String digest, int count, Mapping mapping,
               int rate, String reason, boolean canary, String state, boolean workerEnabled) {}
    record StoredJob(Job job, Preview preview) {}

    final JdbcTemplate db;
    private final ObjectMapper json;
    private final ReplaySource source;
    private final TransactionTemplate transactions;

    Planner(Profiles profiles, ReplaySource source, ObjectMapper json) {
        this.db = profiles.db; this.source = source; this.json = json;
        this.transactions = new TransactionTemplate(new DataSourceTransactionManager(db.getDataSource()));
    }

    <T> T transaction(Supplier<T> operation) { return transactions.execute(status -> operation.get()); }
    String encode(Object value) { try { return json.writeValueAsString(value); } catch (Exception e) { throw new IllegalStateException(e); } }
    <T> T decode(String value, Class<T> type) { try { return json.readValue(value, type); } catch (Exception e) { throw new IllegalStateException(e); } }
    static void required(String value, int max, String name) {
        if (value == null || value.isBlank() || value.length() > max) throw new IllegalArgumentException("Invalid " + name);
    }
    void authorize(Actor actor, Mapping mapping) {
        actor.operator();
        if (!actor.admin() && !mapping.allowedSubjects().isEmpty() && !mapping.allowedSubjects().contains(actor.name()))
            throw new AccessDeniedException("Mapping does not authorize this operator");
    }

    List<Mapping> mappings(Actor actor) {
        actor.operator();
        return db.query("SELECT m.definition FROM replay_mappings m JOIN replay_mapping_heads h ON m.id=h.id AND m.version=h.current_version",
                (r,n) -> decode(r.getString(1), Mapping.class)).stream()
                .filter(m -> actor.admin() || m.allowedSubjects().isEmpty() || m.allowedSubjects().contains(actor.name())).toList();
    }
    Mapping mapping(String id) {
        var rows = db.query("SELECT m.definition FROM replay_mappings m JOIN replay_mapping_heads h ON m.id=h.id AND m.version=h.current_version WHERE m.id=?",
                (r,n) -> decode(r.getString(1), Mapping.class), id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Mapping not found");
        return rows.get(0);
    }

    Mapping saveMapping(String id, MappingInput input, Actor actor) {
        if (!actor.admin()) throw new AccessDeniedException("Admin role required");
        required(input.profileId(), 36, "profile"); required(input.source(), 249, "source topic"); required(input.target(), 249, "destination topic");
        if (!Manifest.EXTRACTOR.equals(input.extractor()) || !Manifest.PARTITION_POLICY.equals(input.partitionPolicy())
                || !Manifest.HEADER_POLICY.equals(input.headerPolicy()) || !input.bytePreservingAttested())
            throw new IllegalArgumentException("Only attested byte-preserving Spring DLT records with v1 policies are supported; envelopes remain browse-only");
        var remove = input.removeHeaders() == null ? List.<String>of() : input.removeHeaders().stream().distinct().sorted().toList();
        var subjects = input.allowedSubjects() == null ? List.<String>of() : input.allowedSubjects().stream().distinct().sorted().toList();
        if (remove.size() > 100 || subjects.size() > 100) throw new IllegalArgumentException("Policy list too large");
        remove.forEach(h -> required(h, 256, "header name")); subjects.forEach(s -> required(s, 256, "subject"));
        var identity = source.identity(input.profileId(), input.source(), input.target());
        return transaction(() -> {
            String mappingId = id == null ? UUID.randomUUID().toString() : id;
            int version = 1;
            if (id == null) db.update("INSERT INTO replay_mapping_heads(id,current_version) VALUES (?,?)", mappingId, version);
            else {
                var versions = db.queryForList("SELECT current_version FROM replay_mapping_heads WHERE id=? FOR UPDATE", Integer.class, id);
                if (versions.isEmpty()) throw new IllegalArgumentException("Mapping not found");
                if (!Objects.equals(input.expectedVersion(), versions.get(0))) throw new Conflict("Mapping changed; reload before editing");
                version = versions.get(0) + 1;
            }
            var saved = new Mapping(mappingId, version, input.profileId(), input.source(), input.target(), input.extractor(),
                    input.partitionPolicy(), input.headerPolicy(), remove, subjects, true, identity);
            db.update("INSERT INTO replay_mappings(id,version,definition) VALUES (?,?,?)", mappingId, version, encode(saved));
            db.update("UPDATE replay_mapping_heads SET current_version=? WHERE id=?", version, mappingId);
            db.update("INSERT INTO browse_audit(actor,action,coordinates) VALUES (?,?,?)", actor.name(), "mapping-version", mappingId + ":" + version);
            return saved;
        });
    }

    PreviewView preview(PreviewRequest request, Actor actor) {
        actor.operator();
        if (request.rate() < 1 || request.rate() > 100000 || request.scanLimit() < 1 || request.scanLimit() > 10000)
            throw new IllegalArgumentException("Rate must be 1..100000 and scanLimit 1..10000");
        var mapping = mapping(request.mappingId()); authorize(actor, mapping);
        checkIdentity(mapping);
        var query = request.filter() == null ? new Browser.Query(null,null,null,null,null,null,null,null,null) : request.filter();
        var scan = source.scan(mapping.profileId(), mapping.source(), query, request.scanLimit());
        checkIdentity(mapping);
        var ordered = scan.records().stream().sorted(Comparator.comparingInt(Browser.Row::partition).thenComparingLong(Browser.Row::offset)).toList();
        var entries = new ArrayList<Manifest.Entry>();
        for (var row : ordered) entries.add(Manifest.extract(row, mapping, entries.size()));
        // Raw source bytes are needed only to compute fingerprints, not duplicated in the stored scan.
        var metadata = new ReplaySource.Scan(List.of(), scan.beginnings(), scan.boundaries(), scan.cursors(), scan.scanned(), scan.partial());
        var preview = new Preview(UUID.randomUUID().toString(), actor.name(), mapping, request.rate(), metadata,
                List.copyOf(entries), Manifest.digest(mapping, request.rate(), entries));
        return transaction(() -> {
            int version = lockMapping(mapping.id());
            if (version != mapping.version()) throw new Conflict("Mapping changed during preview; review again");
            db.update("INSERT INTO replay_previews(id,owner_name,mapping_id,mapping_version,document) VALUES (?,?,?,?,?)",
                    preview.id(), actor.name(), mapping.id(), version, encode(preview));
            db.update("INSERT INTO browse_audit(actor,action,coordinates) VALUES (?,?,?)", actor.name(), "preview", preview.id());
            return view(preview);
        });
    }

    void checkIdentity(Mapping mapping) {
        if (!mapping.identity().equals(source.identity(mapping.profileId(), mapping.source(), mapping.target())))
            throw new Conflict("Cluster or topic identity/partition policy changed; create a new mapping version");
    }
    int lockMapping(String id) { return db.queryForObject("SELECT current_version FROM replay_mapping_heads WHERE id=? FOR UPDATE", Integer.class, id); }
    Preview loadPreview(String id, Actor actor) {
        actor.operator();
        var rows = db.query("SELECT document FROM replay_previews WHERE id=?", (r,n) -> decode(r.getString(1), Preview.class), id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Preview not found");
        var preview = rows.get(0);
        if (!actor.admin() && !preview.owner().equals(actor.name())) throw new AccessDeniedException("Preview belongs to another operator");
        authorize(actor, mapping(preview.mapping().id()));
        return preview;
    }
    PreviewView getPreview(String id, Actor actor) { return view(loadPreview(id, actor)); }
    static PreviewView view(Preview p) {
        return new PreviewView(p.id(), p.mapping(), p.rate(), p.entries().size(), p.digest(), p.scan().scanned(),
                p.scan().partial(), p.scan().beginnings(), p.scan().boundaries(), p.scan().cursors(),
                p.entries().stream().map(e -> new Coordinate(e.position(),e.partition(),e.offset(),e.fingerprint(),e.destinationPartition())).toList(), false);
    }

    Job approve(Approval request, Actor actor) {
        actor.operator(); required(request.idempotencyKey(), 200, "idempotency key");
        required(request.reason(), 2000, "approval reason");
        if (request.canary() == null) throw new IllegalArgumentException("Choose whether to run a one-record canary");
        var preview = loadPreview(request.previewId(), actor);
        if (!preview.digest().equals(request.digest())) throw new Conflict("Manifest digest mismatch; review again");
        if (preview.entries().isEmpty()) throw new IllegalArgumentException("Cannot approve zero records");
        var positions = preview.entries().stream().map(Manifest.Entry::position).toList();
        if (preview.scan().partial() && (!request.acceptPartial() || !positions.equals(request.selectedPositions())))
            throw new IllegalArgumentException("Partial scan requires explicit selection of every displayed manifest position");
        if (request.selectedPositions() != null && !positions.equals(request.selectedPositions()))
            throw new IllegalArgumentException("Approval must match the reviewed manifest exactly; create a narrower preview to change selection");
        String requestHash = Manifest.hash(out -> { Manifest.text(out, encode(request)); Manifest.text(out, actor.name()); });
        try {
            return transaction(() -> {
                int version = lockMapping(preview.mapping().id());
                authorize(actor, mapping(preview.mapping().id()));
                var existing = existingApproval(request.idempotencyKey(), requestHash, actor);
                if (existing != null) return existing;
                if (version != preview.mapping().version()) throw new Conflict("Mapping changed after preview; review a new preview");
                checkIdentity(preview.mapping());
                var job = new Job(UUID.randomUUID().toString(), actor.name(), preview.id(), preview.digest(), preview.entries().size(),
                        preview.mapping(), preview.rate(), request.reason(), request.canary(), JobStates.State.APPROVED.name(), false);
                db.update("INSERT INTO replay_jobs(id,preview_id,owner_name,idempotency_key,request_hash,state,document) VALUES (?,?,?,?,?,?,?)",
                        job.id(), preview.id(), actor.name(), request.idempotencyKey(), requestHash, job.state(), encode(job));
                db.update("INSERT INTO replay_events(job_id,event_type,actor) VALUES (?,?,?)", job.id(), "APPROVED", actor.name());
                return job;
            });
        } catch (org.springframework.dao.DuplicateKeyException e) {
            // Different mapping rows may race on the globally unique key. PostgreSQL rollback precedes re-read.
            var existing = existingApproval(request.idempotencyKey(), requestHash, actor);
            if (existing == null) throw e;
            return existing;
        }
    }
    Job existingApproval(String key, String hash, Actor actor) {
        var rows = db.queryForList("SELECT request_hash,owner_name,document FROM replay_jobs WHERE idempotency_key=?", key);
        if (rows.isEmpty()) return null;
        var row = rows.get(0);
        if (!hash.equals(row.get("request_hash")) || !actor.name().equals(row.get("owner_name")))
            throw new Conflict("Idempotency key already belongs to a different approval request");
        return decode((String)row.get("document"), Job.class);
    }
    StoredJob storedJob(String id, Actor actor) {
        actor.operator();
        var rows = db.query("SELECT document FROM replay_jobs WHERE id=?", (r,n) -> decode(r.getString(1), Job.class), id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Job not found");
        var job = rows.get(0);
        if (!actor.admin() && !job.owner().equals(actor.name())) throw new AccessDeniedException("Job belongs to another operator");
        authorize(actor, mapping(job.mapping().id()));
        return new StoredJob(job, loadPreview(job.previewId(),actor));
    }
    Job job(String id, Actor actor) { return storedJob(id,actor).job(); }
    List<Job> jobs(Actor actor) {
        actor.operator();
        return db.query("SELECT document FROM replay_jobs WHERE owner_name=? OR ?=true ORDER BY approved_at DESC", (r,n) -> decode(r.getString(1),Job.class),actor.name(),actor.admin())
                .stream().filter(j -> { var m=mapping(j.mapping().id());return actor.admin()||m.allowedSubjects().isEmpty()||m.allowedSubjects().contains(actor.name()); }).toList();
    }
    StoredJob verifier(String id, Actor actor) {
        var stored = storedJob(id,actor);
        db.update("INSERT INTO browse_audit(actor,action,coordinates) VALUES (?,?,?)",actor.name(),"manifest-export",id);
        return stored;
    }
    Job release(String id, Actor actor) {
        job(id,actor);
        throw new Conflict("WORKER_DISABLED: release is unavailable until step 3 is accepted");
    }
}
