const dbName = process.env.APP_DB || "movies_db";
db = db.getSiblingDB(dbName);

function ensureUser(user, pwd) {
  try {
    print(`[init-mongo.js] Creating or ensuring user '${user}' in DB '${dbName}' ...`);
    db.createUser({
      user: user,
      pwd: pwd,
      roles: [{ role: "readWrite", db: dbName }]
    });
    print(`[init-mongo.js] User '${user}' created successfully.`);
  } catch (e) {
    if (e.code === 11000) {
      print(`[init-mongo.js] User '${user}' already exists, skipping.`);
    } else {
      print(`[init-mongo.js] Warning while creating user '${user}': ${e}`);
    }
  }
}

const envUser = process.env.APP_USER || "liang";
const envPwd  = process.env.APP_PWD  || "liang";
ensureUser(envUser, envPwd);

ensureUser("jibm23", "nichinonibastardo");

print(`[init-mongo.js] Initialization complete for DB '${dbName}'.`);