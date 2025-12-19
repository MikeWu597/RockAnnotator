const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, '..', 'db', 'database.sqlite');
const db = new sqlite3.Database(dbPath, sqlite3.OPEN_READONLY, (err) => {
  if (err) {
    console.error('Failed to open DB:', err.message);
    process.exit(1);
  }
});

db.all('SELECT id, content, created_at FROM annotations ORDER BY id DESC LIMIT 50', [], (err, rows) => {
  if (err) {
    console.error('Query error:', err.message);
    process.exit(1);
  }

  if (!rows || rows.length === 0) {
    console.log('No annotations found');
    db.close();
    return;
  }

  rows.forEach(r => {
    let parsed = null;
    try {
      parsed = JSON.parse(r.content);
    } catch (e) {
      parsed = { raw: r.content };
    }
    console.log('--- Annotation id:', r.id, 'created_at:', r.created_at, '---');
    console.log(JSON.stringify(parsed, null, 2));
  });

  db.close();
});
