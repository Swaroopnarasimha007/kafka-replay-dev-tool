package dev.replay;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
@SpringBootTest(properties="spring.datasource.url=jdbc:h2:mem:console;MODE=PostgreSQL;DB_CLOSE_DELAY=-1") @AutoConfigureMockMvc
class ConsoleTest {
 @Autowired MockMvc mvc; @Autowired Browser browser;
 @Test void rejectsDashboardUrlsAndBadPorts(){for(String bad:List.of("http://localhost:9092","localhost:0","localhost:65536","localhost:9092,","user@host:9092"))assertThrows(IllegalArgumentException.class,()->Profiles.endpoints(bad));assertEquals(2,Profiles.endpoints("broker-a.example.org:9093,broker-b.example.org:9093").size());}
 @Test void paginationRetainsEveryCoordinate(){var coordinates=new HashSet<String>();Map<Integer,Long> cursors=null;boolean more;do{var p=browser.browse("demo","orders.DLT",new Browser.Query(null,null,null,null,null,null,cursors,7,9),false);for(var r:p.records())assertTrue(coordinates.add(r.partition()+":"+r.offset()));cursors=p.cursors();more=p.truncated();}while(more);assertEquals(60,coordinates.size());}
 @Test void binaryTombstonesDuplicateHeadersAndMasking(){var page=browser.browse("demo","orders.DLT",new Browser.Query(0,null,null,null,null,null,null,10,10),true);assertEquals("AP8B",page.records().get(3).valueBase64());assertNull(page.records().get(4).valueBase64());assertEquals(2,page.records().get(0).headers().stream().filter(h->h.name().equals("trace")).count());var masked=browser.browse("demo","orders.DLT",new Browser.Query(0,null,null,null,null,null,null,1,1),false);assertEquals("[masked]",masked.records().get(0).valueBase64());}
 @Test void filterAndTruncatedScan(){var p=browser.browse("demo","orders.DLT",new Browser.Query(null,null,null,"b3JkZXItMTk=",null,null,null,25,3),false);assertTrue(p.truncated());assertTrue(p.records().isEmpty());assertEquals(3,p.scanned());}
 @Test void apiAndDeepRoute()throws Exception{mvc.perform(get("/api/session")).andExpect(status().isOk()).andExpect(jsonPath("$.mode").value("synthetic demo"));mvc.perform(get("/console/records")).andExpect(forwardedUrl("/index.html"));mvc.perform(post("/api/profiles/demo/topics/orders.DLT/browse").with(csrf()).contentType("application/json").content("{\"limit\":3,\"scanLimit\":3}")).andExpect(status().isOk()).andExpect(jsonPath("$.scanned").value(3));}
}
