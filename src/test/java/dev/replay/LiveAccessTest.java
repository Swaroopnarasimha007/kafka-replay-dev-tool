package dev.replay;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
@SpringBootTest(properties={"spring.datasource.url=jdbc:h2:mem:access;MODE=PostgreSQL;DB_CLOSE_DELAY=-1","spring.datasource.username=sa","spring.datasource.password=", "replay.encryption-key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","replay.allowed-hosts=localhost","spring.security.oauth2.client.registration.console.client-id=test","spring.security.oauth2.client.registration.console.client-secret=test","spring.security.oauth2.client.provider.console.issuer-uri=","spring.security.oauth2.client.provider.console.authorization-uri=https://id.example.test/authorize","spring.security.oauth2.client.provider.console.token-uri=https://id.example.test/token","spring.security.oauth2.client.provider.console.jwk-set-uri=https://id.example.test/jwks","spring.security.oauth2.client.provider.console.user-info-uri=https://id.example.test/userinfo","spring.security.oauth2.client.provider.console.user-name-attribute=sub"})
@ActiveProfiles("live") @AutoConfigureMockMvc
class LiveAccessTest {
 @org.springframework.boot.test.context.TestConfiguration
 static class IdentityFixture {
  @org.springframework.context.annotation.Bean
  org.springframework.security.oauth2.client.registration.ClientRegistrationRepository clients(){
   var client=org.springframework.security.oauth2.client.registration.ClientRegistration.withRegistrationId("console").clientId("test").clientSecret("test").authorizationGrantType(org.springframework.security.oauth2.core.AuthorizationGrantType.AUTHORIZATION_CODE).redirectUri("{baseUrl}/login/oauth2/code/{registrationId}").scope("openid").authorizationUri("https://id.example.test/authorize").tokenUri("https://id.example.test/token").jwkSetUri("https://id.example.test/jwks").userInfoUri("https://id.example.test/userinfo").userNameAttributeName("sub").build();
   return new org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository(client);
  }
 }
 @Autowired MockMvc mvc;
 @Test void viewerCannotRegisterOrReveal()throws Exception {
  mvc.perform(post("/api/profiles/test").with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(post("/api/profiles").with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(post("/api/profiles/id/topics/orders.DLT/reveal").with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(get("/api/profiles").with(user("viewer").roles("Viewer"))).andExpect(status().isOk()).andExpect(content().json("[]"));
  mvc.perform(post("/api/replay/previews").with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(post("/api/replay/approvals").with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(post("/api/replay/mappings").with(user("operator").roles("Operator")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  for(String command:java.util.List.of("start","release","pause","resume"))mvc.perform(post("/api/replay/jobs/id/"+command).with(user("viewer").roles("Viewer")).with(csrf()).contentType("application/json").content("{}")).andExpect(status().isForbidden());
 }
 @Test void noCsrfOrNoRoleIsDenied()throws Exception {
  mvc.perform(post("/api/profiles/test").with(user("admin").roles("Admin")).contentType("application/json").content("{}")).andExpect(status().isForbidden());
  mvc.perform(get("/api/profiles").with(user("unknown").roles("Other"))).andExpect(status().isForbidden());
 }
}
