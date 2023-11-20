package com.example.bigdata;

import com.example.bigdata.pojo.FredColumnPojo;
import com.example.bigdata.util.US_STATES;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.function.Predicate;

public class TempTest {
    @Test
    public void US_STATE_test() {
        US_STATES state = US_STATES.CA;
        System.out.println("----------------");
        System.out.println(state);
        System.out.println(state.getKeyAbbreviation());
        System.out.println(state.getValueFullname());
        System.out.println("----------------");
    }

    @Test
    public void urlTest() throws  Exception{
        String url;
        ObjectMapper mapper = new ObjectMapper();
        url = "https://api.stlouisfed.org/fred/series/search?search_text=Unemployment+Rate+in+Alaska&api_key=b235349016f17660e5a224973626793b&file_type=json";

        JsonNode rootNode = mapper.readTree(new URL(url));
        ArrayNode nodeSeriess = (ArrayNode) rootNode.get("seriess");
        List<FredColumnPojo> listFredData =
                mapper.readValue(nodeSeriess.traverse(), new TypeReference<List<FredColumnPojo>>(){});

        Predicate<FredColumnPojo> predi = (fred) -> (
                fred.getTitle().equals(searchText + state.getValueFullname())
                        && fred.getFrequency_short().charAt(0) == freq.freq
                        && fred.getSeasonal_adjustment_short().equals("NSA")
        );
        url = "https://api.stlouisfed.org/fred/series/observation?series_id=LAUST020000000000003A&api_key=b235349016f17660e5a224973626793b&file_type=json";
        System.out.println(listFredData.size());
    }
}
