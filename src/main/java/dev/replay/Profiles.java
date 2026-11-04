package dev.replay;
import java.util.*;
import java.util.concurrent.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import javax.crypto.*;
import javax.crypto.spec.*;
import org.springframework.stereotype.Service;
import org.springframework.core.env.Environment;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.*;
@Service
class Profiles {
 public boolean isLive(){return live;}
 record Input(String name,String bootstrap,String protocol,String username,String password,String caPem,String replayUsername,String replayPassword) {}
 record Profile(String id,String name,String bootstrap,String clusterId) {}
 record Tested(Input input,String clusterId,long expires) {}
 final boolean live; final Environment env; final JdbcTemplate db; final ObjectMapper json; final byte[] key;
 final Map<String,Tested> tests=new ConcurrentHashMap<>();
 Profiles(Environment env,ObjectProvider<JdbcTemplate> jdbc,ObjectMapper json) {
  this.env=env; this.json=json; live=Arrays.asList(env.getActiveProfiles()).contains("live"); db=jdbc.getIfAvailable();
  if(live&&Arrays.asList(env.getActiveProfiles()).contains("demo"))throw new IllegalStateException("Select only one of live or demo");
  if(live) {
   try {key=Base64.getDecoder().decode(env.getRequiredProperty("replay.encryption-key"));} catch(Exception e){throw new IllegalStateException("Live requires a base64 replay.encryption-key");}
   if(key.length!=32) throw new IllegalStateException("Encryption key must contain 32 bytes");
   if(env.getProperty("replay.allowed-hosts","").isBlank()) throw new IllegalStateException("Live requires replay.allowed-hosts");
   db.execute("CREATE TABLE IF NOT EXISTS cluster_profiles(id varchar(36) PRIMARY KEY,name text NOT NULL,bootstrap text NOT NULL,cluster_id text NOT NULL,secret text NOT NULL)");
   db.execute("CREATE TABLE IF NOT EXISTS browse_audit(id bigserial PRIMARY KEY,at timestamp with time zone DEFAULT now(),actor text,action text,coordinates text)");
  } else key=null;
 }
 static List<String> endpoints(String bootstrap) {
  if(bootstrap==null||bootstrap.isBlank()||bootstrap.length()>4096) throw new IllegalArgumentException("Enter Kafka host:port endpoints, not an HTTP dashboard URL");
  var result=List.of(bootstrap.split(",",-1));
  for(String s:result) { if(!s.matches("[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}")) throw new IllegalArgumentException("Use comma-separated Kafka host:port endpoints; HTTP URLs are invalid"); int port=Integer.parseInt(s.substring(s.lastIndexOf(':')+1)); if(port<1||port>65535) throw new IllegalArgumentException("Invalid broker port"); }
  return result;
 }
 void allowed(String endpoint) {
  endpoints(endpoint); String host=endpoint.substring(0,endpoint.lastIndexOf(':'));
  if(!Arrays.asList(env.getProperty("replay.allowed-hosts","").split(",")).contains(host)) throw new IllegalArgumentException("Broker host is outside deployment allowlist");
 }
 Properties props(Input in) {
  for(String ep:endpoints(in.bootstrap())) allowed(ep);
  Properties p=new Properties(); p.put("bootstrap.servers",in.bootstrap()); p.put("request.timeout.ms","4000"); p.put("default.api.timeout.ms","6000"); p.put("socket.connection.setup.timeout.ms","3000");
  if("PLAINTEXT".equals(in.protocol())) {if(!env.getProperty("replay.allow-plaintext",Boolean.class,false)) throw new IllegalArgumentException("Plaintext requires explicit development configuration");}
  else if("SASL_SSL".equals(in.protocol())) {
   p.put("sasl.mechanism","SCRAM-SHA-512"); p.put("ssl.endpoint.identification.algorithm","https");
   p.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+escape(in.username())+"\" password=\""+escape(in.password())+"\";");
   if(in.caPem()!=null&&!in.caPem().isBlank()) {p.put("ssl.truststore.type","PEM");p.put("ssl.truststore.certificates",in.caPem());}
  } else throw new IllegalArgumentException("Choose SASL_SSL or explicitly enabled PLAINTEXT");
  p.put("security.protocol",in.protocol()); return p;
 }
 static String escape(String v) {if(v==null) throw new IllegalArgumentException("Credentials required");return v.replace("\\","\\\\").replace("\"","\\\"");}
 Map<String,Object> test(Input in) {
  if(!live) throw new IllegalArgumentException("Synthetic demo cannot register real clusters; configure live mode");
  Properties p=props(in);
  try(Admin admin=Admin.create(p)) {
   var description=admin.describeCluster(); String cluster=description.clusterId().get(7,TimeUnit.SECONDS);
   for(var node:description.nodes().get(7,TimeUnit.SECONDS)) {
    String endpoint=node.host()+":"+node.port(); allowed(endpoint);
    var np=new Properties();np.putAll(p);np.put("bootstrap.servers",endpoint);
    try(Admin advertised=Admin.create(np)){ if(!cluster.equals(advertised.describeCluster().clusterId().get(7,TimeUnit.SECONDS))) throw new IllegalArgumentException("Advertised broker cluster mismatch"); }
   }
   String token=UUID.randomUUID().toString(); tests.put(token,new Tested(in,cluster,System.currentTimeMillis()+300000));
   return Map.of("testToken",token,"clusterId",cluster,"expiresInSeconds",300);
  } catch(Exception e){throw failure(e);}
 }
 static IllegalArgumentException failure(Throwable e) {for(Throwable t=e;t!=null;t=t.getCause()){if(t instanceof javax.net.ssl.SSLException||t instanceof SslAuthenticationException)return new IllegalArgumentException("TLS: verify CA and broker hostname");if(t instanceof AuthenticationException)return new IllegalArgumentException("AUTHENTICATION: broker rejected credentials");}return new IllegalArgumentException("CONNECTIVITY: metadata unavailable within timeout; check advertised listeners and allowlist");}
 Profile save(String token) {
  var tested=tests.remove(token); if(tested==null||tested.expires()<System.currentTimeMillis())throw new IllegalArgumentException("Test connection again before saving");
  try {var in=tested.input(); var profile=new Profile(UUID.randomUUID().toString(),in.name(),in.bootstrap(),tested.clusterId());db.update("INSERT INTO cluster_profiles VALUES (?,?,?,?,?)",profile.id(),profile.name(),profile.bootstrap(),profile.clusterId(),encrypt(json.writeValueAsString(in)));return profile;}catch(Exception e){throw new IllegalArgumentException("Profile could not be saved");}
 }
 List<Profile> list(){return live?db.query("SELECT id,name,bootstrap,cluster_id FROM cluster_profiles",(r,n)->new Profile(r.getString(1),r.getString(2),r.getString(3),r.getString(4))):List.of(new Profile("demo","Synthetic incident sandbox","synthetic","demo-cluster"));}
 Input credentials(String id){try {return json.readValue(decrypt(db.queryForObject("SELECT secret FROM cluster_profiles WHERE id=?",String.class,id)),Input.class);}catch(Exception e){throw new IllegalArgumentException("Profile unavailable");}}
 String encrypt(String value)throws Exception {byte[] nonce=new byte[12];new SecureRandom().nextBytes(nonce);var c=Cipher.getInstance("AES/GCM/NoPadding");c.init(Cipher.ENCRYPT_MODE,new SecretKeySpec(key,"AES"),new GCMParameterSpec(128,nonce));return Base64.getEncoder().encodeToString(nonce)+":"+Base64.getEncoder().encodeToString(c.doFinal(value.getBytes(StandardCharsets.UTF_8)));}
 String decrypt(String value)throws Exception {var parts=value.split(":");var c=Cipher.getInstance("AES/GCM/NoPadding");c.init(Cipher.DECRYPT_MODE,new SecretKeySpec(key,"AES"),new GCMParameterSpec(128,Base64.getDecoder().decode(parts[0])));return new String(c.doFinal(Base64.getDecoder().decode(parts[1])),StandardCharsets.UTF_8);}
 void audit(String actor,String action,String coordinates){if(live)db.update("INSERT INTO browse_audit(actor,action,coordinates) VALUES (?,?,?)",actor,action,coordinates);}
}

