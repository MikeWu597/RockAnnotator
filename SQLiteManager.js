const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

class SQLiteManager {
    constructor(dbName = 'database.sqlite') {
        this.dbName = dbName;
        this.db = null;
    }

    /**
     * 初始化数据库连接
     */
    initialize() {
        return new Promise((resolve, reject) => {
            // 确保db目录存在
            const dbDir = path.join(__dirname, 'db');
            if (!fs.existsSync(dbDir)) {
                fs.mkdirSync(dbDir, { recursive: true });
            }

            const dbPath = path.join(dbDir, this.dbName);
            
            this.db = new sqlite3.Database(dbPath, (err) => {
                if (err) {
                    console.error('Error opening database:', err.message);
                    reject(err);
                } else {
                    console.log('Connected to the SQLite database.');
                    this.createTables().then(resolve).catch(reject);
                }
            });
        });
    }

    /**
     * 创建表
     */
    createTables() {
        return new Promise((resolve, reject) => {
            const createTableSQL = `
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS annotations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    content TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                );
                
                CREATE TABLE IF NOT EXISTS images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    width INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    upload_time DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            `;

            this.db.exec(createTableSQL, (err) => {
                if (err) {
                    console.error('Error creating tables:', err.message);
                    reject(err);
                } else {
                    console.log('Tables created or already exist.');
                    resolve();
                }
            });
        });
    }

    /**
     * 插入用户
     */
    insertUser(username, email) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO users (username, email) VALUES (?, ?)';
            this.db.run(sql, [username, email], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
        });
    }

    /**
     * 获取所有用户
     */
    getAllUsers() {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM users';
            this.db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    /**
     * 插入标注
     */
    insertAnnotation(userId, content) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO annotations (user_id, content) VALUES (?, ?)';
            this.db.run(sql, [userId, content], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
        });
    }

    /**
     * 根据用户ID获取标注
     */
    getAnnotationsByUserId(userId) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM annotations WHERE user_id = ?';
            this.db.all(sql, [userId], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    /**
     * 插入图片信息
     */
    insertImage(filename, width, height) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO images (filename, width, height) VALUES (?, ?, ?)';
            this.db.run(sql, [filename, width, height], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
        });
    }

    /**
     * 获取所有图片信息
     */
    getAllImages() {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM images ORDER BY upload_time DESC';
            this.db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    /**
     * 根据ID获取图片信息
     */
    getImageById(id) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM images WHERE id = ?';
            this.db.get(sql, [id], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    /**
     * 关闭数据库连接
     */
    close() {
        return new Promise((resolve, reject) => {
            if (this.db) {
                this.db.close((err) => {
                    if (err) {
                        console.error('Error closing database:', err.message);
                        reject(err);
                    } else {
                        console.log('Database connection closed.');
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        });
    }
}

module.exports = SQLiteManager;