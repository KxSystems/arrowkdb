if[5>.z.K;system"d .arrowkdb"]
clib:$[5<=.z.K;use`.clib;(`arrowkdb 2:(`kexport;1))[]]

clib.dt.printDatatype:{[x] -1 clib.dt.printDatatype_[x];};

clib.fd.printField:{[x] -1 clib.fd.printField_[x];};

clib.sc.printSchema:{[x] -1 clib.sc.printSchema_[x];};

clib.ar.prettyPrintArray:{[x;y;z] -1 clib.ar.prettyPrintArray_[x;y;z];};
clib.ar.prettyPrintArrayFromList:{[list;options] clib.ar.prettyPrintArray[clib.dt.inferDatatype[list];list;options]};

clib.tb.prettyPrintTable:{[x;y;z] -1 clib.tb.prettyPrintTable_[x;y;z];};
clib.tb.prettyPrintTableFromTable:{[table;options] clib.tb.prettyPrintTable[clib.sc.inferSchema[table];value flip table;options]};

clib.orc.writeOrcFromTable:{[filename;table;options] clib.orc.writeOrc[filename;clib.sc.inferSchema[table];value flip table;options]};
clib.orc.readOrcToTable:{[filename;options]
    fields:clib.fd.fieldName each clib.sc.schemaFields[clib.orc.readOrcSchema[filename]];
    data:clib.orc.readOrcData[filename;options];
    $[1~options`WITH_NULL_BITMAP;
        (flip fields!first data;flip fields!last data);
        flip fields!data
        ]
    };

clib.pq.writeParquetFromTable:{[filename;table;options] clib.pq.writeParquet[filename;clib.sc.inferSchema[table];value flip table;options]};
clib.pq.readParquetToTable:{[filename;options] 
    fields:clib.fd.fieldName each clib.sc.schemaFields[clib.pq.readParquetSchema[filename]];
    data:clib.pq.readParquetData[filename;options];
    $[1~options`WITH_NULL_BITMAP;
        (flip fields!first data;flip fields!last data);
        flip fields!data
        ]
    };
clib.pq.readParquetRowGroupsToTable:{[filename;row_groups;columns;options]
    fields:clib.fd.fieldName each clib.sc.schemaFields[clib.pq.readParquetSchema[filename]](columns);
    data:clib.pq.readParquetRowGroups[filename;row_groups;columns;options];
    $[1~options`WITH_NULL_BITMAP;
        (flip fields!first data;flip fields!last data);
        flip fields!data
        ]
    };

clib.ipc.writeArrowFromTable:{[filename;table;options] clib.ipc.writeArrow[filename;clib.sc.inferSchema[table];value flip table;options]};
clib.ipc.readArrowToTable:{[filename;options]
    fields:clib.fd.fieldName each clib.sc.schemaFields[clib.ipc.readArrowSchema[filename]];
    data:clib.ipc.readArrowData[filename;options];
    $[1~options`WITH_NULL_BITMAP;
        (flip fields!first data;flip fields!last data);
        flip fields!data
        ]
    };
clib.ipc.serializeArrowFromTable:{[table;options] clib.ipc.serializeArrow[clib.sc.inferSchema[table];value flip table;options]};
clib.ipc.parseArrowToTable:{[serialized;options] 
    fields:clib.fd.fieldName each clib.sc.schemaFields[clib.ipc.parseArrowSchema[serialized]];
    data:clib.ipc.parseArrowData[serialized;options];
    $[1~options`WITH_NULL_BITMAP;
        (flip fields!first data;flip fields!last data);
        flip fields!data
        ]
    };

if[5<=.z.K;export:(`ts0`ts1 _ clib),([ts:clib.ts0,clib.ts1])]

if[5>.z.K;{set'[key x;value x]}(`ts0`ts1 _ .arrowkdb.clib);ts:clib.ts0,clib.ts1]
