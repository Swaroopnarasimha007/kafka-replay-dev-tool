package dev.replay;
import java.util.*;
import java.security.Principal;
import org.springframework.web.bind.annotation.*;
import org.springframework.stereotype.Controller;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.web.csrf.CsrfToken;
import jakarta.servlet.http.HttpServletRequest;
@RestController @RequestMapping("/api")
class Api {
 final Profiles profiles;final Browser browser;
 Api(Profiles p,Browser b){profiles=p;browser=b;}
 @GetMapping("/session") Map<String,Object> session(HttpServletRequest request, CsrfToken csrf){return Map.of("mode",profiles.live?"live":"synthetic demo","user",request.getUserPrincipal()==null?"demo":request.getUserPrincipal().getName(),"roles",profiles.live?List.of("Viewer","Operator","Admin").stream().filter(request::isUserInRole).toList():List.of("Admin","Operator","Viewer"),"csrfToken",csrf.getToken());}
 @GetMapping("/profiles") @PreAuthorize("@profiles.live == false or hasAnyRole('Viewer','Operator','Admin')") List<Profiles.Profile> profiles(){return profiles.list();}
 @PostMapping("/profiles/test") @PreAuthorize("@profiles.live == false or hasRole('Admin')") Map<String,Object> test(@RequestBody Profiles.Input in){return profiles.test(in);}
 @PostMapping("/profiles") @PreAuthorize("@profiles.live == false or hasRole('Admin')") Profiles.Profile save(@RequestBody Map<String,String> body){return profiles.save(body.get("testToken"));}
 @GetMapping("/profiles/{id}/topics") @PreAuthorize("@profiles.live == false or hasAnyRole('Viewer','Operator','Admin')") List<String> topics(@PathVariable String id){return browser.topics(id);}
 @PostMapping("/profiles/{id}/topics/{topic}/browse") @PreAuthorize("@profiles.live == false or hasAnyRole('Viewer','Operator','Admin')") Browser.Page browse(@PathVariable String id,@PathVariable String topic,@RequestBody Browser.Query q,Principal actor){profiles.audit(actor==null?"demo":actor.getName(),"browse",id+"/"+topic);return browser.browse(id,topic,q,false);}
 @PostMapping("/profiles/{id}/topics/{topic}/reveal") @PreAuthorize("@profiles.live == false or hasAnyRole('Operator','Admin')") Browser.Page reveal(@PathVariable String id,@PathVariable String topic,@RequestBody Browser.Query q,Principal actor){profiles.audit(actor==null?"demo":actor.getName(),"reveal",id+"/"+topic+" "+q);return browser.browse(id,topic,q,true);}
 @ExceptionHandler(IllegalArgumentException.class) @ResponseStatus(org.springframework.http.HttpStatus.BAD_REQUEST) Map<String,String> invalid(IllegalArgumentException e){return Map.of("error",e.getMessage());}
 @ExceptionHandler(org.springframework.security.access.AccessDeniedException.class) @ResponseStatus(org.springframework.http.HttpStatus.FORBIDDEN) Map<String,String> denied(){return Map.of("error","Insufficient privileges");}
 @ExceptionHandler(Exception.class) @ResponseStatus(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR) Map<String,String> error(Exception e){return Map.of("error","Request failed; contact the deployment administrator");}
}
@Controller
class Routes { @GetMapping({"/console","/console/**"}) String ui(){return "forward:/index.html";} }
