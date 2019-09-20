

SET password_encryption = 'scram-sha-256';

CREATE ROLE "test-scram" LOGIN PASSWORD 'test-scram';
GRANT ALL PRIVILEGES ON DATABASE test TO "test-scram";

CREATE ROLE "test-ssl" LOGIN PASSWORD 'test-ssl';
GRANT ALL PRIVILEGES ON DATABASE test TO "test-ssl";

CREATE ROLE "test-ssl-with-cert" LOGIN;
GRANT ALL PRIVILEGES ON DATABASE test TO "test-ssl-with-cert";


select pg_reload_conf();
