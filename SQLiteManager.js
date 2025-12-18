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
                
                CREATE TABLE IF NOT EXISTS annotation_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    completed_at DATETIME NULL,
                    FOREIGN KEY (image_id) REFERENCES images (id)
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
     * 创建标注任务
     */
    createAnnotationTask(imageId) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO annotation_tasks (image_id) VALUES (?)';
            this.db.run(sql, [imageId], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
        });
    }

    /**
     * 获取标注任务列表（带分页）
     */
    getAnnotationTasks(page = 1, pageSize = 10, status = null) {
        return new Promise((resolve, reject) => {
            const offset = (page - 1) * pageSize;
            
            let sql = `
                SELECT 
                    at.id,
                    at.status,
                    at.created_at,
                    at.completed_at,
                    i.filename,
                    i.width,
                    i.height
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
            `;
            
            const params = [];
            
            if (status) {
                sql += ' WHERE at.status = ?';
                params.push(status);
            }
            
            // 默认按ID降序排序
            sql += ' ORDER BY at.id DESC LIMIT ? OFFSET ?';
            params.push(pageSize, offset);
            
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    /**
     * 获取标注任务总数
     */
    getAnnotationTasksCount(status = null) {
        return new Promise((resolve, reject) => {
            let sql = 'SELECT COUNT(*) as count FROM annotation_tasks';
            const params = [];
            
            if (status) {
                sql += ' WHERE status = ?';
                params.push(status);
            }
            
            this.db.get(sql, params, (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row.count);
                }
            });
        });
    }

    /**
     * 根据ID获取标注任务
     */
    getAnnotationTaskById(id) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT 
                    at.id,
                    at.status,
                    at.created_at,
                    at.completed_at,
                    i.filename,
                    i.width,
                    i.height
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
                WHERE at.id = ?
            `;
            
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
     * 获取随机待标注任务
     */
    getRandomPendingTask() {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT 
                    at.id,
                    at.status,
                    at.created_at,
                    i.filename,
                    i.width,
                    i.height
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
                WHERE at.status = 'pending'
                ORDER BY RANDOM()
                LIMIT 1
            `;
            
            this.db.get(sql, [], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    /**
     * 更新任务状态为已完成
     */
    updateTaskStatusToCompleted(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `
                UPDATE annotation_tasks 
                SET status = 'completed', completed_at = datetime('now')
                WHERE id = ?
            `;
            
            this.db.run(sql, [taskId], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    /**
     * 删除标注任务
     */
    deleteAnnotationTaskById(id) {
        return new Promise((resolve, reject) => {
            // 先获取任务信息，以便后续可能需要的清理工作
            const selectSql = `
                SELECT at.image_id, i.filename
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
                WHERE at.id = ?
            `;
            
            this.db.get(selectSql, [id], (err, row) => {
                if (err) {
                    reject(err);
                    return;
                }
                
                if (!row) {
                    resolve(0); // 未找到任务
                    return;
                }
                
                // 删除任务
                const deleteSql = 'DELETE FROM annotation_tasks WHERE id = ?';
                this.db.run(deleteSql, [id], (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        // 同时删除关联的图片记录
                        const deleteImageSql = 'DELETE FROM images WHERE id = ?';
                        this.db.run(deleteImageSql, [row.image_id], (err) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve({
                                    taskId: id,
                                    imageId: row.image_id,
                                    filename: row.filename
                                });
                            }
                        });
                    }
                });
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