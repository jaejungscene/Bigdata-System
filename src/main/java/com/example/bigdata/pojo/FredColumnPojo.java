package com.example.bigdata.pojo;

import com.example.bigdata.util.DefaultLocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import lombok.Data;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.LocalDate;
import java.time.LocalDateTime;


@Data
@JsonPropertyOrder({
        "id", "realtime_start", "realtime_end", "title",
        "observation_start", "observation_end",
        "frequency", "frequency_short", "units", "units_short",
        "seasonal_adjustment", "seasonal_adjustment_short", "last_updated",
        "popularity", "group_popularity", "notes"
})
public class FredColumnPojo {
    @JsonProperty("id")
    private String id; //Series의 ID와 매핑하는 멤버변수입니다.

    @JsonProperty("realtime_start")
    @JsonFormat(pattern = "yyyy-MM-dd")
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate realtime_start;

    @JsonProperty("realtime_end")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate realtime_end;

    @JsonProperty("title")
    private String title; // #E Series© title hIS

    @JsonProperty ("observation_start")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate observation_start;

    @JsonProperty ("observation_end")
    @JsonFormat (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate observation_end;

    @JsonProperty("frequency")
    private String frequency;

    @JsonProperty("frequency_short")
    private String frequency_short;

    @JsonProperty("units")
    private String units;

    @JsonProperty("units_short")
    private String units_short;

    @JsonProperty("seasonal_adjustment")
    private String seasonal_adjustment;

    @JsonProperty("seasonal_adjustment_short")
    private String seasonal_adjustment_short;

    @JsonProperty("last_updated")
    //FRED의 시간대 변수를 실습에 적합한 Time 형식으로 역직렬화 하도록 설정합니다.
    @JsonDeserialize(using = DefaultLocalDateTimeDeserializer.class)
    private LocalDateTime last_updated;

    @JsonProperty("popularity")
    private short popularity;

    @JsonProperty("group_popularity")
    private short group_popularity;

    @JsonProperty("notes")
    private String notes;

    @Override
    public String toString() {
        return "FredColumnPojo [id=" + id + ", realtime_start=" + realtime_start
                + ", realtime_end=" + realtime_end + ", title=" + title
                + ", observation_start=" + observation_start
                + ", observation_end" + observation_end
                + ", frequency=" + frequency + ", frequency_short=" + frequency_short
                + ", units=" + units + ", units_shot=" + units_short
                + ", seasonal_adjustment=" + seasonal_adjustment
                + ", seasonal_adjustment_short=" + seasonal_adjustment_short
                + ", last_updated=" + last_updated
                + ", group_popularity=" + group_popularity
                + ", notes=" + notes + "]";
    }
}
