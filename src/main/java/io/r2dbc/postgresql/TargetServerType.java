package io.r2dbc.postgresql;

import javax.annotation.Nullable;

public enum TargetServerType {
    ANY("any") {
        @Override
        public boolean allowStatus(ClientFactory.HostStatus hostStatus) {
            return hostStatus != ClientFactory.HostStatus.CONNECT_FAIL;
        }
    },
    MASTER("master") {
        @Override
        public boolean allowStatus(ClientFactory.HostStatus hostStatus) {
            return hostStatus == ClientFactory.HostStatus.PRIMARY || hostStatus == ClientFactory.HostStatus.CONNECT_OK;
        }
    },
    SECONDARY("secondary") {
        @Override
        public boolean allowStatus(ClientFactory.HostStatus hostStatus) {
            return hostStatus == ClientFactory.HostStatus.STANDBY || hostStatus == ClientFactory.HostStatus.CONNECT_OK;
        }
    },
    PREFER_SECONDARY("preferSecondary") {
        @Override
        public boolean allowStatus(ClientFactory.HostStatus hostStatus) {
            return hostStatus == ClientFactory.HostStatus.STANDBY || hostStatus == ClientFactory.HostStatus.CONNECT_OK;
        }
    };

    private final String value;

    TargetServerType(String value) {
        this.value = value;
    }

    @Nullable
    public static TargetServerType fromValue(String value) {
        String fixedValue = value.replace("lave", "econdary");
        for (TargetServerType type : values()) {
            if (type.value.equals(fixedValue)) {
                return type;
            }
        }
        return null;
    }

    public String getValue() {
        return value;
    }

    public abstract boolean allowStatus(ClientFactory.HostStatus hostStatus);
}
