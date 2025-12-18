// 更新当前时间显示
function updateTime() {
    const now = new Date();
    const timeString = now.toLocaleString('zh-CN');
    document.getElementById('current-time').textContent = timeString;
}

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化时间显示
    updateTime();
    
    // 每秒更新一次时间
    setInterval(updateTime, 1000);
    
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