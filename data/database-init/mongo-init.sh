#!/bin/sh
mongosh -- "$MONGO_INITDB_DATABASE" <<'EOF'
    var user = process.env.MONGODB_USERNAME;
    var passwd = process.env.MONGODB_PASSWORD;
    var dbName = process.env.MONGO_INITDB_DATABASE;
    db.createUser({user: user, pwd: passwd, roles: [ { role: "dbOwner", db: dbName, } ]});
EOF