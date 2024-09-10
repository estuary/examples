// mongo-init.js

db.createUser({
    user: "adminUser",
    pwd: "adminPassword",
    roles: [
        {
            role: "root",
            db: "admin"  // Replace 'admin' with your desired database
        }
    ]
});
