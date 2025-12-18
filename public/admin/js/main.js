// 更新当前时间显示（仅当存在占位元素时）
function updateTime() {
    const el = document.getElementById('current-time');
    if (!el) return;
    const now = new Date();
    const timeString = now.toLocaleString('zh-CN');
    el.textContent = timeString;
}

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化时间显示（可选）
    updateTime();
    const hasClock = !!document.getElementById('current-time');
    if (hasClock) {
        // 每秒更新一次时间
        setInterval(updateTime, 1000);
    }
    
    // 为所有卡片添加悬停效果
    const cards = document.querySelectorAll('.card');
    cards.forEach(card => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-5px)';
            this.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.1)';
        });
        
        card.addEventListener('mouseleave', function() {
            this.style.transform = '';
            this.style.boxShadow = '';
        });
    });
});

// 动态加载统一的 Admin Navbar 并高亮当前页
document.addEventListener('DOMContentLoaded', async function() {
    const mount = document.getElementById('admin-navbar');
    if (!mount) return;
    try {
        const resp = await fetch('/admin/partials/navbar.html', { cache: 'no-store' });
        const html = await resp.text();
        mount.innerHTML = html;
        const path = window.location.pathname;
        mount.querySelectorAll('.navbar .nav-link').forEach(a => {
            const href = a.getAttribute('href');
            if (href && href === path) {
                a.classList.add('active');
            }
        });
    } catch (e) {
        console.warn('加载导航失败:', e);
    }
});

// 确认退出登录
function confirmLogout(event) {
    event.preventDefault();
    if (confirm('确定要退出登录吗？')) {
        window.location.href = '/admin/logout';
    }
}

// 如果有退出链接，绑定确认事件
document.addEventListener('DOMContentLoaded', function() {
    const logoutLink = document.querySelector('a[href="/admin/logout"]');
    if (logoutLink) {
        logoutLink.addEventListener('click', confirmLogout);
    }
});