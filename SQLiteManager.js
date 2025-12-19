const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

class SQLiteManager {
    constructor(dbName = 'database.sqlite') {
        this.dbName = dbName;
        this.db = null;
    }

    /**
     * 插入一条导出记录
     */
    insertExportRecord(filename, zipPath) {
        return new Promise((resolve, reject) => {
            const sql = "INSERT INTO export_records (filename, zip_path, created_at) VALUES (?, ?, datetime('now','localtime'))";
            this.db.run(sql, [filename, zipPath], function(err) {
                if (err) return reject(err);
                resolve(this.lastID);
            });
        });
    }

    /**
     * 获取导出记录（分页）
     */
    getExportRecords(page = 1, pageSize = 10) {
        return new Promise((resolve, reject) => {
            const offset = (page - 1) * pageSize;
            const sql = `SELECT id, filename, zip_path, created_at FROM export_records ORDER BY id DESC LIMIT ? OFFSET ?`;
            this.db.all(sql, [pageSize, offset], (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }

    /**
     * 获取导出记录总数
     */
    getExportRecordsCount() {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT COUNT(*) as count FROM export_records';
            this.db.get(sql, [], (err, row) => {
                if (err) return reject(err);
                resolve(row ? row.count : 0);
            });
        });
    }

    /**
     * 根据导出记录ID删除记录，返回记录的 zip_path（如果存在）
     */
    deleteExportRecord(id) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT zip_path FROM export_records WHERE id = ?';
            this.db.get(sql, [id], (err, row) => {
                if (err) return reject(err);
                if (!row) return resolve(null);
                const zipPath = row.zip_path;
                const del = 'DELETE FROM export_records WHERE id = ?';
                this.db.run(del, [id], function(delErr) {
                    if (delErr) return reject(delErr);
                    resolve(zipPath);
                });
            });
        });
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
                      .then(() => this.ensureAssignmentColumns())
                      .then(() => this.ensureAnnotatorHeartbeatColumn())
                      .then(resolve)
                      .catch(reject);
                }
            });
        });
    }

    // 确保 annotators 表包含 last_heartbeat 列（兼容旧库）
    ensureAnnotatorHeartbeatColumn() {
        return new Promise((resolve, reject) => {
            const sql = `PRAGMA table_info('annotators')`;
            this.db.all(sql, [], (err, rows) => {
                if (err) return reject(err);
                const has = rows && rows.some(r => r.name === 'last_heartbeat');
                if (has) return resolve();
                const alter = `ALTER TABLE annotators ADD COLUMN last_heartbeat DATETIME NULL`;
                this.db.run(alter, [], (e) => {
                    if (e) return reject(e);
                    resolve();
                });
            });
        });
    }

    // 确保 annotation_tasks 表包含 assigned_to 和 assigned_at 列（兼容旧库）
    ensureAssignmentColumns() {
        return new Promise((resolve, reject) => {
            const sql = `PRAGMA table_info('annotation_tasks')`;
            this.db.all(sql, [], (err, rows) => {
                if (err) return reject(err);
                const hasAssignedTo = rows && rows.some(r => r.name === 'assigned_to');
                const hasAssignedAt = rows && rows.some(r => r.name === 'assigned_at');
                const tasks = [];
                if (!hasAssignedTo) tasks.push("ALTER TABLE annotation_tasks ADD COLUMN assigned_to INTEGER NULL");
                if (!hasAssignedAt) tasks.push("ALTER TABLE annotation_tasks ADD COLUMN assigned_at DATETIME NULL");
                if (tasks.length === 0) return resolve();
                // 顺序执行 ALTER
                const runNext = (i) => {
                    if (i >= tasks.length) return resolve();
                    this.db.run(tasks[i], [], (e) => {
                        if (e) return reject(e);
                        runNext(i+1);
                    });
                };
                runNext(0);
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
                
                CREATE TABLE IF NOT EXISTS export_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    zip_path TEXT NOT NULL,
                    created_at DATETIME DEFAULT (datetime('now','localtime'))
                );
                
                CREATE TABLE IF NOT EXISTS annotators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
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
     * 根据标注员ID获取标注（带分页）
     */
    getAnnotationsByAnnotatorId(annotatorId, page = 1, pageSize = 10) {
        return new Promise((resolve, reject) => {
            const offset = (page - 1) * pageSize;
            const sql = `SELECT id, user_id, content, created_at FROM annotations WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?`;
            this.db.all(sql, [annotatorId, pageSize, offset], (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }

    /**
     * 获取指定标注员的标注总数
     */
    getAnnotationsByAnnotatorCount(annotatorId) {
        return new Promise((resolve, reject) => {
            const sql = `SELECT COUNT(*) as count FROM annotations WHERE user_id = ?`;
            this.db.get(sql, [annotatorId], (err, row) => {
                if (err) return reject(err);
                resolve(row ? row.count : 0);
            });
        });
    }

    /**
     * 获取指定标注员去重后的最新标注（按 taskId 去重，保留每个 task 的最新一条），限制条数
     */
    getLatestUniqueAnnotationsByAnnotator(annotatorId, limit = 5) {
        return new Promise((resolve, reject) => {
            // 先获取该 annotator 的最近若干条标注（按 id desc）以便在内存中按 taskId 去重
            // 取 limit * 10 条作为候选，防止同一任务多次保存时无法凑足 limit 个不同任务
            const candidateCount = Math.max(limit * 10, limit + 10);
            const sql = `SELECT id, user_id, content, created_at FROM annotations WHERE user_id = ? ORDER BY id DESC LIMIT ?`;
            this.db.all(sql, [annotatorId, candidateCount], (err, rows) => {
                if (err) return reject(err);
                try {
                    const unique = [];
                    const seen = new Set();
                    for (const r of (rows || [])) {
                        let parsed = r.content;
                        if (typeof parsed === 'string') {
                            try { parsed = JSON.parse(parsed); } catch (e) { parsed = null; }
                        }
                        const taskId = parsed && (parsed.taskId || parsed.task_id) ? (parsed.taskId || parsed.task_id) : null;
                        const key = taskId !== null ? String(taskId) : (r.id ? `ann_${r.id}` : null);
                        if (key && seen.has(key)) continue;
                        if (key) seen.add(key);
                        unique.push({ id: r.id, created_at: r.created_at, parsedContent: parsed, taskId, filename: null });
                        if (unique.length >= limit) break;
                    }

                    // 为每个 unique 项补上 filename（如果有 taskId）
                    const pending = unique.map(u => {
                        if (!u.taskId) return Promise.resolve(u);
                        return new Promise((res) => {
                            const sql2 = `SELECT i.filename FROM annotation_tasks at JOIN images i ON at.image_id = i.id WHERE at.id = ? LIMIT 1`;
                            this.db.get(sql2, [u.taskId], (err2, row2) => {
                                if (!err2 && row2 && row2.filename) u.filename = row2.filename;
                                res(u);
                            });
                        });
                    });

                    Promise.all(pending).then(results => resolve(results)).catch(reject);
                } catch (e) {
                    reject(e);
                }
            });
        });
    }

    /**
     * 统计指定标注员去重后的任务数（按 taskId 去重）
     */
    getUniqueAnnotatedTaskCount(annotatorId) {
        return new Promise((resolve, reject) => {
            const sql = `SELECT content FROM annotations WHERE user_id = ? ORDER BY id DESC`;
            this.db.all(sql, [annotatorId], (err, rows) => {
                if (err) return reject(err);
                try {
                    const seen = new Set();
                    for (const r of (rows || [])) {
                        let parsed = r.content;
                        if (typeof parsed === 'string') {
                            try { parsed = JSON.parse(parsed); } catch (e) { parsed = null; }
                        }
                        const taskId = parsed && (parsed.taskId || parsed.task_id) ? (parsed.taskId || parsed.task_id) : null;
                        const key = taskId !== null ? String(taskId) : null;
                        if (key) seen.add(key);
                    }
                    resolve(seen.size);
                } catch (e) {
                    reject(e);
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
    getAnnotationTasks(page = 1, pageSize = 10, status = null, filename = null) {
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

            const clauses = [];
            const params = [];

            if (status) {
                clauses.push('at.status = ?');
                params.push(status);
            }

            if (filename) {
                // 模糊匹配文件名（大小写不敏感）
                clauses.push("lower(i.filename) LIKE '%' || lower(?) || '%'");
                params.push(filename);
            }

            if (clauses.length > 0) sql += ' WHERE ' + clauses.join(' AND ');

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
     * 获取匹配条件的所有任务 ID（不分页），用于批量导出 selection
     */
    getAnnotationTaskIds(status = null, filename = null) {
        return new Promise((resolve, reject) => {
            let sql = `SELECT at.id FROM annotation_tasks at JOIN images i ON at.image_id = i.id`;
            const clauses = [];
            const params = [];

            if (status) {
                clauses.push('at.status = ?');
                params.push(status);
            }
            if (filename) {
                clauses.push("lower(i.filename) LIKE '%' || lower(?) || '%'");
                params.push(filename);
            }

            if (clauses.length > 0) sql += ' WHERE ' + clauses.join(' AND ');
            sql += ' ORDER BY at.id DESC';

            this.db.all(sql, params, (err, rows) => {
                if (err) return reject(err);
                const ids = (rows || []).map(r => r.id);
                resolve(ids);
            });
        });
    }

    /**
     * 获取标注任务总数
     */
    getAnnotationTasksCount(status = null, filename = null) {
        return new Promise((resolve, reject) => {
            let sql = 'SELECT COUNT(*) as count FROM annotation_tasks at JOIN images i ON at.image_id = i.id';
            const clauses = [];
            const params = [];

            if (status) {
                clauses.push('at.status = ?');
                params.push(status);
            }

            if (filename) {
                clauses.push("lower(i.filename) LIKE '%' || lower(?) || '%'");
                params.push(filename);
            }

            if (clauses.length > 0) sql += ' WHERE ' + clauses.join(' AND ');

            this.db.get(sql, params, (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row ? row.count : 0);
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

    markTasksUnexported(taskIds) {
        return new Promise((resolve, reject) => {
            if (!Array.isArray(taskIds) || taskIds.length === 0) return resolve(0);
            const placeholders = taskIds.map(() => '?').join(',');
            const sql = `UPDATE annotation_tasks SET exported = 0 WHERE id IN (${placeholders})`;
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
     * 原子性地选取一个未分配的待标注任务并分配给指定标注员
     * 支持超时回收：若任务 assigned_at 已早于当前时间减去 lockMinutes，则可重新分配
     */
    getAndAssignRandomPendingTask(annotatorId, lockMinutes = 30) {
        return new Promise((resolve, reject) => {
            // 优先检查该 annotator 是否已有分配的 pending 任务（可继续编辑）
            const assignedSql = `
                SELECT at.id
                FROM annotation_tasks at
                JOIN images i ON at.image_id = i.id
                WHERE at.status = 'pending' AND at.assigned_to = ?
                LIMIT 1
            `;
            this.db.get(assignedSql, [annotatorId], (err, row) => {
                if (err) return reject(err);
                if (row && row.id) {
                    const taskId = row.id;
                    const sql = `
                        SELECT at.id, at.status, at.created_at, at.assigned_to, at.assigned_at, i.filename, i.width, i.height
                        FROM annotation_tasks at
                        JOIN images i ON at.image_id = i.id
                        WHERE at.id = ?
                    `;
                    this.db.get(sql, [taskId], (gErr, taskRow) => {
                        if (gErr) return reject(gErr);
                        return resolve(taskRow);
                    });
                    return;
                }

                // 允许通过 annotators.last_heartbeat 控制：如果 assigned_to 的 annotator 有最近心跳，则不应被重新分配
                const selectSql = `
                    SELECT at.id
                    FROM annotation_tasks at
                    LEFT JOIN annotators a ON at.assigned_to = a.id
                    JOIN images i ON at.image_id = i.id
                    WHERE at.status = 'pending'
                      AND (
                          at.assigned_to IS NULL
                          OR datetime(at.assigned_at) <= datetime('now', '-' || ? || ' minutes')
                          OR a.last_heartbeat IS NULL
                          OR datetime(a.last_heartbeat) <= datetime('now', '-' || ? || ' minutes')
                      )
                    ORDER BY RANDOM()
                    LIMIT 1
                `;
                // 参数： lockMinutes, heartbeatMinutes (use same value for heartbeat window default)
                const heartbeatMinutes = lockMinutes >= 1 ? Math.min(5, Math.max(1, Math.floor(lockMinutes / 15))) : 2;
                this.db.get(selectSql, [String(lockMinutes), String(heartbeatMinutes)], (err2, row2) => {
                if (err) return reject(err);
                if (err2) return reject(err2);
                if (!row2) return resolve(null);
                const taskId = row2.id;
                const upd = `UPDATE annotation_tasks SET assigned_to = ?, assigned_at = datetime('now','localtime') WHERE id = ?`;
                this.db.run(upd, [annotatorId, taskId], function(updErr) {
                    if (updErr) return reject(updErr);
                    // 返回任务详情
                    const sql = `
                        SELECT at.id, at.status, at.created_at, at.assigned_to, at.assigned_at, i.filename, i.width, i.height
                        FROM annotation_tasks at
                        JOIN images i ON at.image_id = i.id
                        WHERE at.id = ?
                    `;
                    this.db.get(sql, [taskId], (gErr, taskRow) => {
                        if (gErr) return reject(gErr);
                        resolve(taskRow);
                    });
                }.bind(this));
            });
            });
        });
    }

    /**
     * 更新标注员心跳时间
     */
    updateAnnotatorHeartbeat(annotatorId) {
        return new Promise((resolve, reject) => {
            const sql = `UPDATE annotators SET last_heartbeat = datetime('now','localtime') WHERE id = ?`;
            this.db.run(sql, [annotatorId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    /**
     * 释放指定标注员的所有 pending 任务的分配（用于页面关闭）
     */
    releaseAssignmentsForAnnotator(annotatorId) {
        return new Promise((resolve, reject) => {
            const sql = `UPDATE annotation_tasks SET assigned_to = NULL, assigned_at = NULL WHERE assigned_to = ? AND status = 'pending'`;
            this.db.run(sql, [annotatorId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    assignTaskToAnnotator(taskId, annotatorId) {
        return new Promise((resolve, reject) => {
            const sql = `UPDATE annotation_tasks SET assigned_to = ?, assigned_at = datetime('now','localtime') WHERE id = ?`;
            this.db.run(sql, [annotatorId, taskId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
            });
        });
    }

    releaseTaskAssignment(taskId) {
        return new Promise((resolve, reject) => {
            const sql = `UPDATE annotation_tasks SET assigned_to = NULL, assigned_at = NULL WHERE id = ?`;
            this.db.run(sql, [taskId], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
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
                SET status = 'completed', completed_at = datetime('now','localtime'), assigned_to = NULL, assigned_at = NULL
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
     * 创建标注员账号（管理员维护）
     */
    createAnnotator(username, passwordHash) {
        return new Promise((resolve, reject) => {
            const sql = 'INSERT INTO annotators (username, password_hash, created_at) VALUES (?, ?, datetime(\'now\',\'localtime\'))';
            this.db.run(sql, [username, passwordHash], function(err) {
                if (err) return reject(err);
                resolve(this.lastID);
            });
        });
    }

    /**
     * 获取标注员列表（分页）
     */
    getAnnotators(page = 1, pageSize = 10) {
        return new Promise((resolve, reject) => {
            const offset = (page - 1) * pageSize;
            const sql = 'SELECT id, username, created_at FROM annotators ORDER BY id DESC LIMIT ? OFFSET ?';
            this.db.all(sql, [pageSize, offset], (err, rows) => {
                if (err) return reject(err);
                resolve(rows || []);
            });
        });
    }

    /**
     * 获取标注员总数
     */
    getAnnotatorsCount() {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT COUNT(*) as count FROM annotators';
            this.db.get(sql, [], (err, row) => {
                if (err) return reject(err);
                resolve(row ? row.count : 0);
            });
        });
    }

    /**
     * 根据ID获取标注员
     */
    getAnnotatorById(id) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT id, username, created_at FROM annotators WHERE id = ?';
            this.db.get(sql, [id], (err, row) => {
                if (err) return reject(err);
                resolve(row || null);
            });
        });
    }

    /**
     * 根据用户名获取标注员（包含密码Hash，用于鉴权）
     */
    getAnnotatorByUsername(username) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM annotators WHERE username = ?';
            this.db.get(sql, [username], (err, row) => {
                if (err) return reject(err);
                resolve(row || null);
            });
        });
    }

    /**
     * 更新标注员信息（可更新用户名或密码Hash）
     */
    updateAnnotator(id, username, passwordHash = null) {
        return new Promise((resolve, reject) => {
            if (passwordHash) {
                const sql = 'UPDATE annotators SET username = ?, password_hash = ? WHERE id = ?';
                this.db.run(sql, [username, passwordHash, id], function(err) {
                    if (err) return reject(err);
                    resolve(this.changes);
                });
            } else {
                const sql = 'UPDATE annotators SET username = ? WHERE id = ?';
                this.db.run(sql, [username, id], function(err) {
                    if (err) return reject(err);
                    resolve(this.changes);
                });
            }
        });
    }

    /**
     * 删除标注员
     */
    deleteAnnotator(id) {
        return new Promise((resolve, reject) => {
            const sql = 'DELETE FROM annotators WHERE id = ?';
            this.db.run(sql, [id], function(err) {
                if (err) return reject(err);
                resolve(this.changes);
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