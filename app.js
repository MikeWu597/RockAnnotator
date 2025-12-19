const express = require('express');
const YAML = require('yamljs');
const path = require('path');
const SQLiteManager = require('./SQLiteManager');
const session = require('express-session');
const SQLiteStore = require('connect-sqlite3')(session);
const multer = require('multer');
const { imageSize } = require('image-size');
const fs = require('fs');
const { exportDrSong } = require('./scripts/dr_song_engine');

// 加载配置文件
const config = YAML.load(path.join(__dirname, 'config.yml'));

// 创建Express应用
const app = express();
const port = process.env.PORT || config.server.port;

// 增加payload大小限制
app.use(express.json({ limit: '500mb' }));
app.use(express.urlencoded({ extended: true, limit: '500mb' }));

// 设置静态资源目录
app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));
// 提供下载目录静态访问
app.use('/downloads', express.static(path.join(__dirname, 'downloads')));

// 配置multer用于文件上传
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/')
  },
  filename: function (req, file, cb) {
    // 保持原始文件名
    cb(null, file.originalname)
  }
});

// 创建两个multer实例，分别处理单文件和多文件上传
const singleUpload = multer({ 
  storage: storage,
  fileFilter: function (req, file, cb) {
    // 只允许jpg和png格式
    if (file.mimetype === 'image/jpeg' || file.mimetype === 'image/png') {
      cb(null, true);
    } else {
      cb(new Error('只允许上传JPG和PNG格式的图片'));
    }
  },
  limits: {
    fileSize: 50 * 1024 * 1024, // 限制文件大小为50MB
    files: 1000 // 限制文件数量为1000个
  }
});

const batchUpload = multer({ 
  storage: storage,
  fileFilter: function (req, file, cb) {
    // 只允许jpg和png格式
    if (file.mimetype === 'image/jpeg' || file.mimetype === 'image/png') {
      cb(null, true);
    } else {
      cb(new Error('只允许上传JPG和PNG格式的图片'));
    }
  },
  limits: {
    fileSize: 50 * 1024 * 1024, // 限制文件大小为50MB
    files: 1000 // 限制文件数量为1000个
  }
});

// 配置Session存储
app.use(session({
  secret: 'rockannotator_secret_key',
  resave: false,
  saveUninitialized: false,
  store: new SQLiteStore({
    dir: path.join(__dirname, 'db'),
    db: 'sessions.db',
    table: 'user_sessions'
  }),
  cookie: { 
    secure: false, // 在生产环境中如果使用HTTPS则设置为true
    maxAge: 24 * 60 * 60 * 1000 // 24小时
  }
}));

// 创建SQLite管理器实例，使用配置中的数据库名
const dbManager = new SQLiteManager(config.database.name);

// 在服务器启动时初始化数据库
async function initializeDatabase() {
  try {
    await dbManager.initialize();
    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Failed to initialize database:', error);
    process.exit(1);
  }
}

// 中间件：检查是否已认证
function isAuthenticated(req, res, next) {
  if (req.session && req.session.admin) {
    return next();
  } else {
    res.redirect('/admin/login');
  }
}

// 路由定义
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 管理员登录页面
app.get('/admin/login', (req, res) => {
  // 如果已经登录，则重定向到管理面板
  if (req.session && req.session.admin) {
    return res.redirect('/admin/dashboard');
  }
  
  // 提供静态登录页面
  res.sendFile(path.join(__dirname, 'public', 'admin', 'login.html'));
});

// 处理管理员登录
app.post('/admin/login', (req, res) => {
  const { password } = req.body;
  
  // 检查密码是否正确
  if (password === config.admin.password) {
    // 设置session
    req.session.admin = true;
    req.session.save(() => {
      res.redirect('/admin/dashboard');
    });
  } else {
    // 密码错误，返回登录页并显示错误
    res.status(401).send(`
      <!DOCTYPE html>
      <html lang="zh-CN">
      <head>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>登录失败</title>
          <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
      </head>
      <body class="bg-light">
          <div class="container">
              <div class="row justify-content-center mt-5">
                  <div class="col-md-6">
                      <div class="alert alert-danger" role="alert">
                          密码错误！请重试。
                      </div>
                      <div class="d-grid gap-2">
                          <a href="/admin/login" class="btn btn-primary">返回登录</a>
                      </div>
                  </div>
              </div>
          </div>
      </body>
      </html>
    `);
  }
});

// 管理面板主页
app.get('/admin/dashboard', isAuthenticated, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin', 'dashboard.html'));
});

// 图片上传页面
app.get('/admin/upload', isAuthenticated, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin', 'upload.html'));
});

// 标注任务页面
app.get('/admin/tasks', isAuthenticated, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin', 'tasks.html'));
});

// 数据导出页面
app.get('/admin/export', isAuthenticated, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin', 'export.html'));
});

// 管理员：数据导出（Dr. Song JSON 格式）
app.get('/api/admin/export', isAuthenticated, async (req, res) => {
  try {
    const start = req.query.start || null; // ISO
    const end = req.query.end || null;     // ISO

    const uploadsDir = path.join(__dirname, 'uploads');
    const downloadsDir = path.join(__dirname, 'downloads');
    const tmpRootDir = path.join(__dirname, 'tmp');

    // 确保目录存在
    if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
    if (!fs.existsSync(downloadsDir)) fs.mkdirSync(downloadsDir, { recursive: true });
    if (!fs.existsSync(tmpRootDir)) fs.mkdirSync(tmpRootDir, { recursive: true });

    // 将传入的时间适配为 UTC ISO，兼容客户端发送的本地时间（无时区）或带时区的 ISO
    function parseToUTCISO(input) {
      if (!input) return null;
      // 如果包含时区信息（Z 或 ±hh:mm），直接使用 Date 解析为 UTC
      if (/Z|[+-]\d{2}:?\d{2}$/.test(input)) {
        const d = new Date(input);
        if (isNaN(d)) return null;
        return d.toISOString();
      }

      // 否则按东八区（UTC+8）解释输入的本地时间（格式 YYYY-MM-DDTHH:mm 或类似），转换为 UTC
      // 解析数字组件
      const m = input.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
      if (!m) {
        // 回退到 Date 解析
        const d2 = new Date(input);
        if (isNaN(d2)) return null;
        return d2.toISOString();
      }
      const y = parseInt(m[1], 10);
      const mo = parseInt(m[2], 10);
      const da = parseInt(m[3], 10);
      const hh = parseInt(m[4], 10);
      const mi = parseInt(m[5], 10);
      // 本地（东八） -> UTC = local - 8h
      const utcMillis = Date.UTC(y, mo - 1, da, hh, mi) - (8 * 60 * 60 * 1000);
      return new Date(utcMillis).toISOString();
    }

    const startISO = parseToUTCISO(start);
    const endISO = parseToUTCISO(end);
    const onlyUnexported = req.query.onlyUnexported === '1' || req.query.onlyUnexported === 'true';

    const zipAbsPath = await exportDrSong({
      dbManager,
      startISO: startISO,
      endISO: endISO,
      uploadsDir,
      downloadsDir,
      tmpRootDir,
      onlyUnexported
    });

    // 将绝对路径转换为可下载 URL 路径
    const zipFileName = path.basename(zipAbsPath);
    const zipUrlPath = `/downloads/${zipFileName}`;

    // 记录导出记录到数据库，便于重复下载
    try {
      await dbManager.insertExportRecord(zipFileName, zipUrlPath);
    } catch (recErr) {
      console.warn('Failed to record export record:', recErr.message || recErr);
    }

    res.json({ success: true, data: { zipPath: zipUrlPath } });
  } catch (error) {
    console.error('Export error:', error);
    res.status(500).json({ success: false, error: error.message || '导出失败' });
  }
});

// 管理员：导出预览（返回将要导出的任务列表，但不实际导出）
app.get('/api/admin/export/preview', isAuthenticated, async (req, res) => {
  try {
    const start = req.query.start || null; // ISO or local
    const end = req.query.end || null;

    // parseToUTCISO 重复逻辑与导出接口一致
    function parseToUTCISO(input) {
      if (!input) return null;
      if (/Z|[+-]\d{2}:?\d{2}$/.test(input)) {
        const d = new Date(input);
        if (isNaN(d)) return null;
        return d.toISOString();
      }
      const m = input.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
      if (!m) {
        const d2 = new Date(input);
        if (isNaN(d2)) return null;
        return d2.toISOString();
      }
      const y = parseInt(m[1], 10);
      const mo = parseInt(m[2], 10);
      const da = parseInt(m[3], 10);
      const hh = parseInt(m[4], 10);
      const mi = parseInt(m[5], 10);
      const utcMillis = Date.UTC(y, mo - 1, da, hh, mi) - (8 * 60 * 60 * 1000);
      return new Date(utcMillis).toISOString();
    }

    const startISO = parseToUTCISO(start);
    const endISO = parseToUTCISO(end);
    const onlyUnexported = req.query.onlyUnexported === '1' || req.query.onlyUnexported === 'true';

    let tasks = [];
    if (onlyUnexported) {
      tasks = await dbManager.getUnexportedCompletedTasksInRange(startISO, endISO);
    } else {
      tasks = await dbManager.getCompletedTasksInRange(startISO, endISO);
    }

    res.json({ success: true, data: tasks });
  } catch (error) {
    console.error('Preview error:', error);
    res.status(500).json({ success: false, error: error.message || '预览失败' });
  }
});

// 管理员：获取导出记录（带分页）
app.get('/api/admin/export/records', isAuthenticated, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 10;

    const records = await dbManager.getExportRecords(page, pageSize);
    const total = await dbManager.getExportRecordsCount();

    res.json({
      success: true,
      data: {
        records,
        pagination: {
          page,
          pageSize,
          total,
          totalPages: Math.ceil(total / pageSize)
        }
      }
    });
  } catch (err) {
    console.error('Error fetching export records:', err);
    res.status(500).json({ success: false, error: err.message || '查询导出记录失败' });
  }
});

// 处理单个图片上传的API
app.post('/api/admin/images/upload', isAuthenticated, singleUpload.single('image'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: '请选择要上传的图片' });
    }
    
    // 读取文件并获取图片尺寸信息
    const buffer = fs.readFileSync(req.file.path);
    const dimensions = imageSize(buffer);
    
    // 保存图片信息到数据库
    const imageId = await dbManager.insertImage(req.file.filename, dimensions.width, dimensions.height);
    
    // 为上传的图片创建标注任务
    await dbManager.createAnnotationTask(imageId);
    
    res.json({
      success: true,
      message: '图片上传成功',
      data: {
        id: imageId,
        filename: req.file.filename,
        width: dimensions.width,
        height: dimensions.height,
        size: req.file.size
      }
    });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 处理批量图片上传的API
app.post('/api/admin/images/upload/batch', isAuthenticated, batchUpload.array('images', 1000), async (req, res) => {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: '请选择要上传的图片' });
    }
    
    const results = {
      success: [],
      failed: []
    };
    
    // 顺序处理每个文件
    for (const file of req.files) {
      try {
        // 读取文件并获取图片尺寸信息
        const buffer = fs.readFileSync(file.path);
        const dimensions = imageSize(buffer);
        
        // 保存图片信息到数据库
        const imageId = await dbManager.insertImage(file.filename, dimensions.width, dimensions.height);
        
        // 为上传的图片创建标注任务
        await dbManager.createAnnotationTask(imageId);
        
        results.success.push({
          id: imageId,
          filename: file.filename,
          width: dimensions.width,
          height: dimensions.height,
          size: file.size
        });
      } catch (error) {
        console.error('Batch upload error for file:', file.filename, error);
        results.failed.push({
          filename: file.filename,
          error: error.message
        });
      }
    }
    
    res.json({
      success: true,
      message: `成功上传 ${results.success.length} 个文件，失败 ${results.failed.length} 个`,
      data: results
    });
  } catch (error) {
    console.error('Batch upload error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 获取所有图片的API
app.get('/api/admin/images', isAuthenticated, async (req, res) => {
  try {
    const images = await dbManager.getAllImages();
    res.json({
      success: true,
      data: images
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 获取标注任务列表的API（带分页）
app.get('/api/admin/tasks', isAuthenticated, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 10;
    const status = req.query.status || null;
    
    const tasks = await dbManager.getAnnotationTasks(page, pageSize, status);
    const total = await dbManager.getAnnotationTasksCount(status);
    
    res.json({
      success: true,
      data: {
        tasks,
        pagination: {
          page,
          pageSize,
          total,
          totalPages: Math.ceil(total / pageSize)
        }
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 获取单个标注任务详情的API
app.get('/api/admin/tasks/:id', isAuthenticated, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id);
    const task = await dbManager.getAnnotationTaskById(taskId);
    
    if (!task) {
      return res.status(404).json({
        success: false,
        error: '任务未找到'
      });
    }

    // 查询该任务相关的 annotations（如果有）
    let annotations = [];
    try {
      annotations = await dbManager.getAnnotationsByTaskId(taskId);
    } catch (err) {
      console.warn('Failed to load annotations for task', taskId, err.message);
    }
    
    res.json({
      success: true,
      data: Object.assign({}, task, { annotations })
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 获取单个任务的 annotations（独立接口，便于前端单独请求）
app.get('/api/admin/tasks/:id/annotations', isAuthenticated, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id);
    const annotations = await dbManager.getAnnotationsByTaskId(taskId);
    res.json({ success: true, data: annotations });
  } catch (error) {
    console.error('Error fetching annotations for task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// 删除标注任务的API
app.delete('/api/admin/tasks/:id', isAuthenticated, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id);
    const result = await dbManager.deleteAnnotationTaskById(taskId);
    
    if (result === 0) {
      return res.status(404).json({
        success: false,
        error: '任务未找到'
      });
    }
    
    // 尝试删除上传的文件
    try {
      const filePath = path.join(__dirname, 'uploads', result.filename);
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
    } catch (fileError) {
      console.warn('Failed to delete file:', fileError.message);
    }
    
    res.json({
      success: true,
      message: '任务删除成功'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 管理员：重置任务（删除标注信息并重置为 pending）
app.post('/api/admin/tasks/:id/reset', isAuthenticated, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id);

    // 删除与该任务相关的 annotations
    await dbManager.deleteAnnotationsByTaskId(taskId);

    // 将任务状态设置为 pending
    await dbManager.resetTaskToPending(taskId);

    res.json({ success: true });
  } catch (error) {
    console.error('Error resetting task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// 管理员：重置任务导出状态（设置 exported = 0）
app.post('/api/admin/tasks/:id/unexport', isAuthenticated, async (req, res) => {
  try {
    const taskId = parseInt(req.params.id);
    const changes = await dbManager.markTaskUnexported(taskId);
    res.json({ success: true, changes });
  } catch (error) {
    console.error('Error unexporting task:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// 获取随机待标注任务的API
app.get('/api/tasks/random', async (req, res) => {
  try {
    // 获取一个随机的待标注任务
    const task = await dbManager.getRandomPendingTask();
    
    if (!task) {
      return res.json({
        success: false,
        error: '没有待标注的任务'
      });
    }
    
    const imagePath = `/uploads/${task.filename}`;

    res.json({
      success: true,
      data: {
        task: {
          id: task.id,
          status: task.status,
          createdAt: task.created_at
        },
        imagePath: imagePath
      }
    });
  } catch (error) {
    console.error('Error getting random task:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 公共：获取标签列表（用于前端标注时弹窗）
app.get('/api/tags', async (req, res) => {
  try {
    const tags = await dbManager.getAllTags();
    res.json({ success: true, data: tags });
  } catch (error) {
    console.error('Error fetching tags:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// 管理员：标签管理 API
app.get('/api/admin/tags', isAuthenticated, async (req, res) => {
  try {
    const tags = await dbManager.getAllTags();
    res.json({ success: true, data: tags });
  } catch (error) {
    console.error('Error fetching admin tags:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/admin/tags', isAuthenticated, async (req, res) => {
  try {
    const { name } = req.body;
    if (!name) return res.status(400).json({ success: false, error: '缺少标签名称' });
    const id = await dbManager.addTag(name);
    res.json({ success: true, data: { id, name } });
  } catch (error) {
    console.error('Error creating tag:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.put('/api/admin/tags/:id', isAuthenticated, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const { name } = req.body;
    if (!name) return res.status(400).json({ success: false, error: '缺少标签名称' });
    await dbManager.updateTag(id, name);
    res.json({ success: true });
  } catch (error) {
    console.error('Error updating tag:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.delete('/api/admin/tags/:id', isAuthenticated, async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    await dbManager.deleteTag(id);
    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting tag:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// 管理页面路由：标签管理
app.get('/admin/tags', isAuthenticated, (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin', 'tags.html'));
});


// 保存标注的API
app.post('/api/annotations/save', async (req, res) => {
  try {
    const { taskId, polygons } = req.body;
    
    if (!taskId || !polygons) {
      return res.status(400).json({
        success: false,
        error: '缺少必要参数'
      });
    }

    // 将标注内容以 JSON 存入 annotations 表中
    const content = JSON.stringify({ taskId, polygons });
    await dbManager.insertAnnotation(null, content);

    // 更新任务状态为已完成
    await dbManager.updateTaskStatusToCompleted(taskId);
    
    res.json({
      success: true,
      message: '标注已保存'
    });
  } catch (error) {
    console.error('Error saving annotation:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// 管理员登出
app.get('/admin/logout', (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error('Session destruction error:', err);
    }
    res.redirect('/admin/login');
  });
});

// 获取所有用户
app.get('/api/users', async (req, res) => {
  try {
    const users = await dbManager.getAllUsers();
    res.json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 创建新用户
app.post('/api/users', async (req, res) => {
  try {
    const { username, email } = req.body;
    if (!username || !email) {
      return res.status(400).json({ error: 'Username and email are required' });
    }
    
    const userId = await dbManager.insertUser(username, email);
    res.status(201).json({ id: userId, username, email });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 获取特定用户的标注
app.get('/api/users/:userId/annotations', async (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    const annotations = await dbManager.getAnnotationsByUserId(userId);
    res.json(annotations);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 为用户创建标注
app.post('/api/users/:userId/annotations', async (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    const { content } = req.body;
    
    if (!content) {
      return res.status(400).json({ error: 'Content is required' });
    }
    
    const annotationId = await dbManager.insertAnnotation(userId, content);
    res.status(201).json({ id: annotationId, userId, content });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// 管理员认证中间件
function authenticateAdmin(req, res, next) {
  const { password } = req.body;
  if (!password || password !== config.admin.password) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// 受保护的管理端点示例
app.post('/api/admin/protected', authenticateAdmin, (req, res) => {
  res.json({ message: 'Access granted to protected resource' });
});

// 启动服务器
async function startServer() {
  // 先初始化数据库
  await initializeDatabase();
  
  // 启动Express服务器
  const server = app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
    console.log(`Admin panel is available at http://localhost:${port}/admin/login`);
    console.log(`Admin password: ${config.admin.password}`);
  });

  // 优雅关闭处理
  process.on('SIGINT', async () => {
    console.log('\nShutting down gracefully...');
    await dbManager.close();
    server.close(() => {
      console.log('Server closed');
      process.exit(0);
    });
  });
}

// 启动应用
startServer().catch(error => {
  console.error('Failed to start server:', error);
  process.exit(1);
});

module.exports = app;