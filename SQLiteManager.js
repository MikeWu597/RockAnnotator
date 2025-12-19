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
                    this.createTables()
                      .then(() => this.ensureExportedColumn())
                      .then(resolve)
                      .catch(reject);
                }
            });
        });
    }

    // 确保 annotation_tasks 表包含 exported 列（兼容旧库）
    ensureExportedColumn() {
        return new Promise((resolve, reject) => {
            const sql = `PRAGMA table_info('annotation_tasks')`;
            this.db.all(sql, [], (err, rows) => {
                if (err) return reject(err);
                const has = rows && rows.some(r => r.name === 'exported');
                if (has) return resolve();
                const alter = `ALTER TABLE annotation_tasks ADD COLUMN exported INTEGER DEFAULT 0`;
                this.db.run(alter, [], (e) => {
                    if (e) return reject(e);
                    resolve();
                });
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
                    created_at DATETIME DEFAULT (datetime('now','localtime'))
                );
                
                CREATE TABLE IF NOT EXISTS annotations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    content TEXT NOT NULL,
                    created_at DATETIME DEFAULT (datetime('now','localtime')),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                );
                
                CREATE TABLE IF NOT EXISTS images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    width INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    upload_time DATETIME DEFAULT (datetime('now','localtime'))
                );
                
                CREATE TABLE IF NOT EXISTS annotation_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    exported INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT (datetime('now','localtime')),
                    completed_at DATETIME NULL,
                    FOREIGN KEY (image_id) REFERENCES images (id)
                );

                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    created_at DATETIME DEFAULT (datetime('now','localtime'))
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
            const sql = "INSERT INTO annotations (user_id, content, created_at) VALUES (?, ?, datetime('now','localtime'))";
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
     * 获取所有标签
     */
    getAllTags() {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM tags ORDER BY name ASC';
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
     * 添加标签
     */
    addTag(name) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO tags (name) VALUES (?)';
            this.db.run(sql, [name], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
        });
    }

    /**
     * 更新标签
     */
    updateTag(id, name) {
        return new Promise((resolve, reject) => {
            const sql = 'UPDATE tags SET name = ? WHERE id = ?';
            this.db.run(sql, [name, id], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    /**
     * 删除标签
     */
    deleteTag(id) {
        return new Promise((resolve, reject) => {
            const sql = 'DELETE FROM tags WHERE id = ?';
            this.db.run(sql, [id], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
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
            const sql = "INSERT INTO images (filename, width, height, upload_time) VALUES (?, ?, ?, datetime('now','localtime'))";
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
            const sql = "INSERT INTO annotation_tasks (image_id, created_at) VALUES (?, datetime('now','localtime'))";
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
                    at.exported,
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
                    at.exported,
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
     * 获取时间范围内完成的任务（含图片信息）
     */
    getCompletedTasksInRange(startISO, endISO) {
        return new Promise((resolve, reject) => {
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
                WHERE at.status = 'completed'
            `;

            const params = [];
            if (startISO) {
                sql += ` AND datetime(at.completed_at) >= datetime(?)`;
                params.push(startISO);
            }
            if (endISO) {
                sql += ` AND datetime(at.completed_at) <= datetime(?)`;
                params.push(endISO);
            }
            // 支持仅未导出过滤（按传入参数实现）
            // 如果 caller 需要过滤 exported，请在 SQL 外层调用或使用 getCompletedTasksInRangeWithExported
            sql += ' ORDER BY at.completed_at ASC';

            this.db.all(sql, params, (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }

    /**
     * 获取时间范围内完成且未导出的任务（含图片信息）
     */
    getUnexportedCompletedTasksInRange(startISO, endISO) {
        return new Promise((resolve, reject) => {
            let sql = `
                SELECT 
                    at.id,
                    at.status,
                    at.exported,
                    at.created_at,
                    at.completed_at,
                    i.filename,
                    i.width,
                    i.height
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
                WHERE at.status = 'completed' AND (at.exported IS NULL OR at.exported = 0)
            `;

            const params = [];
            if (startISO) {
                sql += ` AND datetime(at.completed_at) >= datetime(?)`;
                params.push(startISO);
            }
            if (endISO) {
                sql += ` AND datetime(at.completed_at) <= datetime(?)`;
                params.push(endISO);
            }
            sql += ' ORDER BY at.completed_at ASC';

            this.db.all(sql, params, (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }

    /**
     * 标记一组任务为已导出
     */
    markTasksExported(taskIds) {
        return new Promise((resolve, reject) => {
            if (!Array.isArray(taskIds) || taskIds.length === 0) return resolve(0);
            const placeholders = taskIds.map(() => '?').join(',');
            const sql = `UPDATE annotation_tasks SET exported = 1 WHERE id IN (${placeholders})`;
            this.db.run(sql, taskIds, function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    /**
     * 标记单个任务为未导出（exported = 0）
     */
    markTaskUnexported(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `UPDATE annotation_tasks SET exported = 0 WHERE id = ?`;
            this.db.run(sql, [taskId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
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
                SET status = 'completed', completed_at = datetime('now','localtime')
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

    // 将任务重置为待标注状态
    resetTaskToPending(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `
                UPDATE annotation_tasks
                SET status = 'pending', completed_at = NULL
                WHERE id = ?
            `;

            this.db.run(sql, [taskId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    // 删除与某任务相关的 annotations（以 content 字段中包含 taskId 为匹配依据）
    deleteAnnotationsByTaskId(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `
                DELETE FROM annotations
                WHERE instr(content, '"taskId":' || ?) > 0
                   OR instr(content, '"taskId": ' || ?) > 0
            `;

            this.db.run(sql, [String(taskId), String(taskId)], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    // 根据任务ID获取相关的 annotations（content 字段解析为 JSON）
    getAnnotationsByTaskId(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT content FROM annotations
                WHERE instr(content, '"taskId":' || ?) > 0
                   OR instr(content, '"taskId": ' || ?) > 0
            `;

            this.db.all(sql, [String(taskId), String(taskId)], (err, rows) => {
                if (err) return reject(err);
                try {
                    const parsed = rows.map(r => JSON.parse(r.content));
                    resolve(parsed);
                } catch (parseErr) {
                    reject(parseErr);
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