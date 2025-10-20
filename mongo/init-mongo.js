db = db.getSiblingDB(process.env.APP_DB || "movies_db");
db.createUser({
    user: process.env.APP_USER || "liang",
    pwd: process.env.APP_PWD || "liang",
    roles: [{ role: "readWrite", db: process.env.APP_DB || "movies_db" }]
});
