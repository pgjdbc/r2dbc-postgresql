package io.r2dbc.postgresql;

import javax.annotation.Nullable;

public enum TargetServerType {
    ANY("any") {
        @Override
        public boolean allowStatus(MultiHostConnectionStrategy.HostStatus hostStatus) {
            return hostStatus != MultiHostConnectionStrategy.HostStatus.CONNECT_FAIL;
        }
    },
    MASTER("master") {
        @Override
        public boolean allowStatus(MultiHostConnectionStrategy.HostStatus hostStatus) {
            return hostStatus == MultiHostConnectionStrategy.HostStatus.PRIMARY || hostStatus == MultiHostConnectionStrategy.HostStatus.CONNECT_OK;
        }
    },
    SECONDARY("secondary") {
        @Override
        public boolean allowStatus(MultiHostConnectionStrategy.HostStatus hostStatus) {
            return hostStatus == MultiHostConnectionStrategy.HostStatus.STANDBY || hostStatus == MultiHostConnectionStrategy.HostStatus.CONNECT_OK;
        }
    },
    PREFER_SECONDARY("preferSecondary") {
        @Override
        public boolean allowStatus(MultiHostConnectionStrategy.HostStatus hostStatus) {
            return hostStatus == MultiHostConnectionStrategy.HostStatus.STANDBY || hostStatus == MultiHostConnectionStrategy.HostStatus.CONNECT_OK;
        }
    };

    private final String value;

    TargetServerType(String value) {
        this.value = value;
    }

    @Nullable
    public static TargetServerType fromValue(String value) {
        for (TargetServerType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        return null;
    }

    public String getValue() {
        return value;
    }

    public abstract boolean allowStatus(MultiHostConnectionStrategy.HostStatus hostStatus);

}
