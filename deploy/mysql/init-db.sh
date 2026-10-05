#!/bin/sh
set -e

# The original MySQL 5.6 dump contains zero-date defaults. Relax only this import session.
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --protocol=socket --user=root \
    --database="$MYSQL_DATABASE" --default-character-set=utf8mb4 \
    --init-command="SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'" \
    < /bootstrap/hmdp.sql

MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --protocol=socket --user=root \
    --database="$MYSQL_DATABASE" < /bootstrap/upgrade.sql

MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --protocol=socket --user=root \
    --database="$MYSQL_DATABASE" < /bootstrap/close-refund-upgrade.sql
