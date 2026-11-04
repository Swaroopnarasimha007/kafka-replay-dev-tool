package dev.replay;
import java.util.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
@Configuration @EnableMethodSecurity
class Security {
 @Bean @Profile("lab")
 org.springframework.security.core.userdetails.UserDetailsService labUsers(Environment env) {
  String password=env.getRequiredProperty("replay.lab-password");
  if(password.length()<12)throw new IllegalStateException("Lab password must have at least 12 characters");
  return new org.springframework.security.provisioning.InMemoryUserDetailsManager(
   org.springframework.security.core.userdetails.User.withUsername(env.getProperty("replay.lab-user","lab"))
    .password(org.springframework.security.crypto.factory.PasswordEncoderFactories.createDelegatingPasswordEncoder().encode(password)).roles("Admin").build());
 }

 @Bean SecurityFilterChain chain(HttpSecurity http, Environment env) throws Exception {
  boolean live=Arrays.asList(env.getActiveProfiles()).contains("live");
  if(!live && !Arrays.asList(env.getDefaultProfiles()).contains("demo") && !Arrays.asList(env.getActiveProfiles()).contains("demo")) throw new IllegalStateException("Select demo or live profile");
  http.csrf(c->c.csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse()).csrfTokenRequestHandler(new org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler()));
  if(Arrays.asList(env.getActiveProfiles()).contains("lab")) {
   http.authorizeHttpRequests(a->a.anyRequest().authenticated()).httpBasic(org.springframework.security.config.Customizer.withDefaults());
  } else if(live) {
   var service=new OidcUserService();
   http.authorizeHttpRequests(a->a.requestMatchers("/","/assets/**","/console/**","/error").permitAll().anyRequest().authenticated())
    .oauth2Login(o->o.userInfoEndpoint(u->u.oidcUserService(request->{var user=service.loadUser(request); var roles=user.getClaimAsStringList("roles"); var authorities=new ArrayList<org.springframework.security.core.GrantedAuthority>(); if(roles!=null) for(String role:roles) if(Set.of("Viewer","Operator","Admin").contains(role)) authorities.add(new SimpleGrantedAuthority("ROLE_"+role)); return new DefaultOidcUser(authorities,user.getIdToken(),user.getUserInfo());})));
  } else http.authorizeHttpRequests(a->a.anyRequest().permitAll());
  return http.build();
 }
}
