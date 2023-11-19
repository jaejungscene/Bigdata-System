package com.example.bigdata;

import com.example.bigdata.util.US_STATES;
import org.junit.jupiter.api.Test;

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
}
