\l ../q/arrowkdb.q

N:5
dset_tab:([]tstamp:asc N?0p;temperature:N?100f;fill_level:N?100;pump_status:N?0b;comment:N?("start";"stop";"alert";"acknowledge"; ""))

getListSet:{
    []
    n:(1?5)[0]+1;
    enlist (n?("start";"stop";"alert";"acknowledge"; ""))
    }

y:getListSet[]
x:5
while[x-:1;y:y,getListSet[]]

tstamp_fd:.arrowkdb.fd.field[`timestamp;.arrowkdb.dt.timestamp[`nano]];
temp_fd:.arrowkdb.fd.field[`temperature;.arrowkdb.dt.float64[]];
fill_fd:.arrowkdb.fd.field[`fill_level;.arrowkdb.dt.int64[]];
sensor_fd:.arrowkdb.fd.field[`sensor_data;.arrowkdb.dt.struct[(temp_fd,fill_fd)]];
pump_fd:.arrowkdb.fd.field[`pump_status;.arrowkdb.dt.boolean[]];
comment_fd:.arrowkdb.fd.field[`comment;.arrowkdb.dt.list[.arrowkdb.dt.utf8[]]];

schema:.arrowkdb.sc.schema[(tstamp_fd,sensor_fd,pump_fd,comment_fd)];

.arrowkdb.sc.printSchema[schema]

N:5
dset:(asc N?0p;(N?100f;N?100);N?0b;y)