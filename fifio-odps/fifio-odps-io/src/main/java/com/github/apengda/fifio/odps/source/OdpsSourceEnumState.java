package com.github.apengda.fifio.odps.source;

import java.util.Set;

public class OdpsSourceEnumState {
    private final Set<String> assignedPartitions;

    OdpsSourceEnumState(Set<String> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }

    public Set<String> assignedPartitions() {
        return assignedPartitions;
    }

}
