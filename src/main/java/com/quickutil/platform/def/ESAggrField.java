package com.quickutil.platform.def;

import java.util.Map;

import com.google.gson.JsonObject;

public class ESAggrField {

    public class ESTermField extends ESAggrField {
        String termField;
        int size;
        String sortField;
        String order;
    }

    public class ESDateField extends ESAggrField {
        String dateField;
        String interval;
        String timeZone;
        long min;
        long max;
    }

    public class ESCalculateField extends ESAggrField {
        Map<String, String> fieldMap;
    }

    public JsonObject toJsonObject() {
        if (this instanceof ESTermField) {
            ESTermField field = (ESTermField) this;
            JsonObject object1 = new JsonObject();
            object1.addProperty(field.sortField, field.order);
            JsonObject object2 = new JsonObject();
            object2.add("order", object1);
            object2.addProperty("field", field.termField);
            object2.addProperty("size", field.size);
            JsonObject object3 = new JsonObject();
            object3.add("terms", object2);
            JsonObject object4 = new JsonObject();
            object4.add(field.termField, object3);
            JsonObject object5 = new JsonObject();
            object5.add("aggs", object4);
            return object5;
        } else if (this instanceof ESDateField) {
            ESDateField field = (ESDateField) this;
            JsonObject object1 = new JsonObject();
            object1.addProperty("min", field.min);
            object1.addProperty("max", field.max);
            JsonObject object2 = new JsonObject();
            object2.add("extended_bounds", object1);
            object2.addProperty("field", field.dateField);
            object2.addProperty("interval", field.interval);
            object2.addProperty("time_zone", field.timeZone);
            JsonObject object3 = new JsonObject();
            object3.add("date_histogram", object2);
            JsonObject object4 = new JsonObject();
            object4.add(field.dateField, object3);
            JsonObject object5 = new JsonObject();
            object5.add("aggs", object4);
        } else if (this instanceof ESCalculateField) {
            ESCalculateField field = (ESCalculateField) this;
            JsonObject object1 = new JsonObject();
            for (String fieldname : field.fieldMap.keySet()) {
                JsonObject object2 = new JsonObject();
                object2.addProperty("field", fieldname);
                JsonObject object3 = new JsonObject();
                object3.add(field.fieldMap.get(fieldname), object2);
                object1.add(fieldname, object3);
            }
            JsonObject object4 = new JsonObject();
            object4.add("aggs", object1);
            return object4;
        }
        return null;
    }

}
