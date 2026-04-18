package org.ncsu.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import org.ncsu.entity.Action;
import org.ncsu.entity.ActionRecord;
import org.ncsu.entity.Strategy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ActionRecordService {


    public ActionRecord createActionRecord(ActionRecord actionRecord, Response response, Action action) {

        System.out.println("Action record: " + response);
        Map<String, Object> responseBody = response.readEntity(Map.class);
        System.out.println("responseBody: " + responseBody);
        String timestampStr = (String) responseBody.get("timestamp");
        Instant instant = Instant.parse(timestampStr);
        actionRecord.setTimestamp(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
        actionRecord.setStatus(response.getStatus());
        actionRecord.setAction(action);

        return actionRecord;
    }
}
