const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Jimp = require('jimp');
const AdmZip = require('adm-zip');
// 使用 Jimp 获取图片尺寸，避免直接调用 image-size 导致 Buffer 类型问题

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function uuidv4() {
  // 生成简洁的 UUID v4
  return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
    (c ^ crypto.randomBytes(1)[0] & 15 >> c / 4).toString(16)
  );
}

function toArrayPoints(polygons) {
  if (!Array.isArray(polygons)) return [];
  return polygons
    .filter(p => Array.isArray(p.points) && p.points.length >= 3)
    .map(p => ({
      label: p.tag || '',
      points: p.points.map(pt => [Number(pt.x), Number(pt.y)]),
      shape_type: 'polygon'
    }));
}

async function processImageToTemp(srcFilePath, destStemPath) {
  const ext = path.extname(srcFilePath).toLowerCase();
  const destImgPath = ext === '.png' ? destStemPath + '.jpg' : destStemPath + '.jpg';
  // 统一导出为 jpg，若原为 jpg 则重编码；若为 png 则转换
  const img = await Jimp.read(srcFilePath);
  await img.quality(95).writeAsync(destImgPath);
  return destImgPath;
}

function fileToBase64(filePath) {
  const buf = fs.readFileSync(filePath);
  return buf.toString('base64');
}

async function buildOneLabelmeJson({ task, annotations, destImgPath, uuidName }) {
  const jimg = await Jimp.read(destImgPath);
  const dim = { width: jimg.bitmap.width, height: jimg.bitmap.height };
  // 取最后一条或合并？按现有前端逻辑，使用最后一条记录中的 polygons
  let polygons = [];
  if (Array.isArray(annotations) && annotations.length > 0) {
    const last = annotations[annotations.length - 1];
    if (last && Array.isArray(last.polygons)) polygons = last.polygons;
  }
  const shapes = toArrayPoints(polygons).map(s => ({
    label: s.label || '',
    line_color: null,
    fill_color: null,
    points: s.points,
    shape_type: 'polygon',
    flags: {}
  }));

  const json = {
    version: '3.16.2',
    flags: {},
    shapes,
    lineColor: [0, 255, 0, 128],
    fillColor: [255, 0, 0, 128],
    imagePath: `${uuidName}.jpg`,
    imageData: fileToBase64(destImgPath),
    imageHeight: dim.height || task.height || null,
    imageWidth: dim.width || task.width || null
  };
  return json;
}

async function exportDrSong({ dbManager, startISO, endISO, uploadsDir, downloadsDir, tmpRootDir, onlyUnexported = true }) {
  ensureDir(downloadsDir);
  ensureDir(tmpRootDir);

  // 创建临时导出目录
  const batchId = `${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
  const tmpDir = path.join(tmpRootDir, `export-${batchId}`);
  ensureDir(tmpDir);

  try {
    // 根据 onlyUnexported 决定查询未导出或全部已完成任务
    let tasks = [];
    if (onlyUnexported) {
      tasks = await dbManager.getUnexportedCompletedTasksInRange(startISO, endISO);
    } else {
      tasks = await dbManager.getCompletedTasksInRange(startISO, endISO);
    }
    if (!tasks || tasks.length === 0) {
      // 空结果：也打包空 zip，保持流程一致
      const emptyZipName = `export_${batchId}.zip`;
      const emptyZipPath = path.join(downloadsDir, emptyZipName);
      const zip = new AdmZip();
      zip.writeZip(emptyZipPath);
      fs.rmSync(tmpDir, { recursive: true, force: true });
      return emptyZipPath;
    }

    const processedTaskIds = [];
    for (const task of tasks) {
      const srcPath = path.join(uploadsDir, task.filename);
      if (!fs.existsSync(srcPath)) {
        // 跳过缺失的源文件
        continue;
      }
      const uuidName = uuidv4();
      const destStem = path.join(tmpDir, uuidName);
      const destImgPath = await processImageToTemp(srcPath, destStem);

      // 读取标注
      let anns = [];
      try {
        anns = await dbManager.getAnnotationsByTaskId(task.id);
      } catch (_) {}

      const json = await buildOneLabelmeJson({ task, annotations: anns, destImgPath, uuidName });
      const jsonPath = path.join(tmpDir, `${uuidName}.json`);
      fs.writeFileSync(jsonPath, JSON.stringify(json, null, 2), 'utf-8');
      processedTaskIds.push(task.id);
    }

    // 压缩临时目录
    const zipName = `export_${batchId}.zip`;
    const zipPath = path.join(downloadsDir, zipName);
    const zip = new AdmZip();
    zip.addLocalFolder(tmpDir);
    zip.writeZip(zipPath);

    // 清理临时目录
    fs.rmSync(tmpDir, { recursive: true, force: true });
    // 标记已导出的任务
    try {
      if (processedTaskIds.length > 0) await dbManager.markTasksExported(processedTaskIds);
    } catch (e) {
      console.warn('Failed to mark tasks exported:', e.message || e);
    }

    return zipPath;
  } catch (err) {
    // 出错也尝试清理
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
    throw err;
  }
}

module.exports = { exportDrSong };
