const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, '..', 'db', 'database.sqlite');
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error('Failed to open DB:', err.message);
    process.exit(1);
  }
});

db.all(`SELECT at.id as taskId, at.status, at.created_at, at.completed_at, i.id as imageId, i.filename, i.width, i.height
FROM annotation_tasks at
JOIN images i ON at.image_id = i.id
ORDER BY at.id DESC LIMIT 100`, [], (err, rows) => {
  if (err) {
    console.error('Query error:', err.message);
    process.exit(1);
  }

  if (!rows || rows.length === 0) {
    console.log('No tasks found');
    db.close();
    return;
  }

  rows.forEach(r => {
    console.log(`Task ${r.taskId}: status=${r.status} filename=${r.filename} size=${r.width}x${r.height} imageId=${r.imageId}`);
  });

  db.close();
});
