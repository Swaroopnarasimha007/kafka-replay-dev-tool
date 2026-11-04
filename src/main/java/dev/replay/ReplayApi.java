package dev.replay;

import java.util.*;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;

@RestController
@RequestMapping("/api/replay")
@PreAuthorize("@profiles.live == false or hasAnyRole('Operator','Admin')")
class ReplayApi {
    private final Planner planner;
    private final Profiles profiles;
    ReplayApi(Planner planner, Profiles profiles) { this.planner=planner; this.profiles=profiles; }

    Planner.Actor actor(Authentication authentication) {
        if (!profiles.live) return new Planner.Actor("demo",Set.of("Viewer","Operator","Admin"));
        if (authentication == null) throw new AccessDeniedException("Login required");
        var roles = new HashSet<String>();
        authentication.getAuthorities().forEach(a -> { if(a.getAuthority().startsWith("ROLE_")) roles.add(a.getAuthority().substring(5)); });
        return new Planner.Actor(authentication.getName(),Set.copyOf(roles));
    }
    @GetMapping("/mappings") List<Planner.Mapping> mappings(Authentication auth) { return planner.mappings(actor(auth)); }
    @PostMapping("/mappings") @PreAuthorize("@profiles.live == false or hasRole('Admin')")
    Planner.Mapping create(@RequestBody Planner.MappingInput input, Authentication auth) { return planner.saveMapping(null,input,actor(auth)); }
    @PostMapping("/mappings/{id}/versions") @PreAuthorize("@profiles.live == false or hasRole('Admin')")
    Planner.Mapping revise(@PathVariable String id, @RequestBody Planner.MappingInput input, Authentication auth) { return planner.saveMapping(id,input,actor(auth)); }
    @PostMapping("/previews") Planner.PreviewView preview(@RequestBody Planner.PreviewRequest request, Authentication auth) { return planner.preview(request,actor(auth)); }
    @GetMapping("/previews/{id}") Planner.PreviewView preview(@PathVariable String id, Authentication auth) { return planner.getPreview(id,actor(auth)); }
    @PostMapping("/approvals") Planner.Job approve(@RequestBody Planner.Approval request, Authentication auth) { return planner.approve(request,actor(auth)); }
    @GetMapping("/jobs") List<Planner.Job> jobs(Authentication auth) { return planner.jobs(actor(auth)); }
    @GetMapping("/jobs/{id}") Planner.Job job(@PathVariable String id, Authentication auth) { return planner.job(id,actor(auth)); }
    @GetMapping("/jobs/{id}/verifier") Planner.StoredJob verifier(@PathVariable String id, Authentication auth) { return planner.verifier(id,actor(auth)); }
    @PostMapping("/jobs/{id}/release") Planner.Job release(@PathVariable String id, Authentication auth) { return planner.release(id,actor(auth)); }
    @GetMapping("/state-contract") Map<String,Object> states() {
        return Map.of("workerEnabled",false,"initial","APPROVED","transitions",JobStates.TRANSITIONS,
                "release","Idempotent AWAITING_RELEASE → RUNNING; unavailable while worker is disabled");
    }
}

@RestControllerAdvice(assignableTypes=ReplayApi.class)
class ReplayErrors {
    @ExceptionHandler(Planner.Conflict.class) @ResponseStatus(HttpStatus.CONFLICT)
    Map<String,String> conflict(Planner.Conflict e) { return Map.of("error",e.getMessage()); }
    @ExceptionHandler(AccessDeniedException.class) @ResponseStatus(HttpStatus.FORBIDDEN)
    Map<String,String> denied() { return Map.of("error","Insufficient mapping or role authorization"); }
    @ExceptionHandler({IllegalArgumentException.class,org.springframework.http.converter.HttpMessageNotReadableException.class}) @ResponseStatus(HttpStatus.BAD_REQUEST)
    Map<String,String> invalid(Exception e) { return Map.of("error",e instanceof IllegalArgumentException?e.getMessage():"Invalid request body"); }
    @ExceptionHandler(Exception.class) @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    Map<String,String> error() { return Map.of("error","Request failed; contact the deployment administrator"); }
}
