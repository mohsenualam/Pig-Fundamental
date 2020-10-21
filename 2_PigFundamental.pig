truck_events = LOAD '/user/malam/truck/truck_event_text_partition.csv' USING PigStorage(',') AS (driverId:int, truckId:int, eventTime:chararray, eventType:chararray, longitude:double, latitude:double, eventKey:chararray, correlationId:long, driverName:chararray, routeId:long,routeName:chararray,eventDate:chararray);

truck_events_subset = LIMIT truck_events 100;

dump truck_events_subset;

specific_columns = FOREACH truck_events_subset GENERATE driverId, eventTime, eventType;

STORE specific_columns INTO 'output/specific_columns' USING PigStorage(',');


hdfs dfs -cat output/specific_columns/part-r-00000 | head


drivers = LOAD '/user/malam/truck/drivers.csv' USING PigStorage(',') AS (driverId:int, name:chararray, ssn:chararray, location:chararray, certified:chararray, wage_plan:chararray);

join_data = JOIN truck_events BY (driverId), drivers BY (driverId);

ordered_data = ORDER drivers BY name asc;

DUMP ordered_data;

filtered_events = FILTER truck_events BY NOT (eventType MATCHES 'Normal');

grouped_events = GROUP filtered_events BY driverId;

DESCRIBE grouped_events;

DUMP grouped_events;