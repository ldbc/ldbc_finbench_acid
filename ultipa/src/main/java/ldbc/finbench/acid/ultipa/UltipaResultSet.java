package ldbc.finbench.acid.ultipa;

import com.ultipa.Ultipa;
import com.ultipa.sdk.operate.entity.DataItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UltipaResultSet {
    com.ultipa.sdk.operate.response.Response response;

    public UltipaResultSet(com.ultipa.sdk.operate.response.Response map) {
        this.response = map;
    }

    boolean isOk() {
        return Optional.ofNullable(response).map(m -> m.getStatus()).map(m -> m.getErrorCode() == Ultipa.ErrorCode.SUCCESS).orElse(false);
    }

    int count() {
        if (!response.getItems().isEmpty()) {
            return count(response.get(0));
        }
        return 0;
    }

    private int count(DataItem dataItem) {
        return Optional.ofNullable(dataItem.getEntities()).map(m -> m.size()).orElse(0);
    }

    boolean isEmpty(String alias) {
        DataItem dataItem = response.getItems().get(alias);
        if (dataItem == null) {
            return true;
        }
        return count(dataItem) == 0;
    }

    long aliasAsLong(String alias) {
        DataItem dataItem = response.getItems().get(alias);
        if (dataItem == null) {
            return 0;
        }
        List<Object> entities = dataItem.getEntities();
        if (entities == null) {
            return 0;
        }
        if (entities.size() == 1) {
            Map<String, Object> map = (Map) entities.get(0);
            List<Object> values = (List<Object>) map.get("values");
            if (values != null) {
                if (values.size() == 1) {
                    Object value = values.get(0);
                    return (long) Double.parseDouble(value.toString());
                }
            }
        }
        return 0;
    }


    <T>
    List<T> aliasAsList(String alias, List<T> defaultList, Class<T> clazz) {
        DataItem dataItem = response.alias(alias);
        if (dataItem == null) {
            return defaultList;
        }
        List<Object> entities = dataItem.getEntities();

        if (entities != null) {
            assert entities.size() == 1;

            List<T> resList = new ArrayList<>();
            for (Object e : entities) {
                Map<String, Object> map = (Map<String, Object>) e;
                List<Object> values = (List) map.getOrDefault("values", defaultList);
                if (values == null) {
                    return defaultList;
                }
                for (Object v : values) {
                    if (clazz == Long.class) {
                        resList.add(clazz.cast((long) Double.parseDouble(v.toString())));
                        continue;
                    }
                    resList.add(clazz.cast(v));
                }
            }
            return resList;
        }
        return defaultList;
    }

    @Override
    public String toString() {
        String innerMessage = null;
        if (response != null) {
            innerMessage = Optional.ofNullable(response).map(m -> m.getStatus()).map(m -> m.toString()).orElse("null");
        }
        return "UltipaResultSet{" +
                "response=" + innerMessage +
                '}';
    }
}
