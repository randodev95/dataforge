// TITAN Dashboard Logic - Interactivity Layer
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    populateMockData();
    setupEventListeners();
});

function setupEventListeners() {
    // Navigation & View Switching
    const navItems = document.querySelectorAll('.nav-links li');
    const views = document.querySelectorAll('.view');

    navItems.forEach(item => {
        item.addEventListener('click', () => {
            const targetView = item.getAttribute('data-view');
            
            // Update Navigation UI
            navItems.forEach(i => i.classList.remove('active'));
            item.classList.add('active');

            // Switch Views
            views.forEach(v => {
                v.classList.remove('active');
                if (v.id === `${targetView}-view`) {
                    v.classList.add('active');
                }
            });

            // Trigger view-specific logic
            if (targetView === 'lineage') {
                renderLineage('fct_orders');
            }
        });
    });

    // Env Selector
    document.getElementById('target-env').addEventListener('change', (e) => {
        console.log(`Switching environment to ${e.target.value}`);
        populateMockData();
    });

    // Lineage Selector
    document.getElementById('lineage-model-select')?.addEventListener('change', (e) => {
        renderLineage(e.target.value);
    });
}

function initCharts() {
    const canvas = document.getElementById('throughputChart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(0, 242, 255, 0.2)');
    gradient.addColorStop(1, 'rgba(0, 242, 255, 0)');

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['08:00', '10:00', '12:00', '14:00', '16:00', '18:00', '20:00'],
            datasets: [{
                label: 'Rows/Sec',
                data: [450, 620, 580, 890, 1100, 950, 1200],
                borderColor: '#00f2ff',
                borderWidth: 2,
                pointBackgroundColor: '#00f2ff',
                fill: true,
                backgroundColor: gradient,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#8a8d91', font: { family: 'IBM Plex Mono' } } },
                x: { grid: { display: false }, ticks: { color: '#8a8d91', font: { family: 'IBM Plex Mono' } } }
            }
        }
    });
}

function renderLineage(modelName) {
    const canvas = document.getElementById('lineage-canvas');
    if (!canvas) return;

    // Mock lineage data
    const lineageData = {
        'fct_orders': {
            sources: ['stg_orders', 'stg_payments'],
            columns: [
                { name: 'order_id', source: 'stg_orders.id' },
                { name: 'amount', source: 'stg_payments.amount' },
                { name: 'status', source: 'stg_orders.status' }
            ]
        },
        'dim_users': {
            sources: ['stg_users'],
            columns: [
                { name: 'user_id', source: 'stg_users.id' },
                { name: 'full_name', source: 'stg_users.name' }
            ]
        }
    };

    const data = lineageData[modelName];
    if (!data) return;

    canvas.innerHTML = `
        <div class="lineage-graph">
            <div class="lineage-node source">
                <h4>SOURCES</h4>
                <div class="column-list">
                    ${data.sources.map(s => `<div class="column-item"><span>${s}</span></div>`).join('')}
                </div>
            </div>
            <div class="lineage-arrow"></div>
            <div class="lineage-node target">
                <h4>${modelName.toUpperCase()}</h4>
                <div class="column-list">
                    ${data.columns.map(c => `
                        <div class="column-item">
                            <span>${c.name}</span>
                            <span style="opacity:0.4">← ${c.source.split('.')[1]}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        </div>
    `;
}

function populateMockData() {
    // Stats
    const totalModels = document.getElementById('stat-total-models');
    if (totalModels) totalModels.innerText = '42';
    
    const avgLatency = document.getElementById('stat-avg-latency');
    if (avgLatency) avgLatency.innerText = '1,240ms';

    // Recent Models
    const models = [
        { name: 'fct_orders', status: 'success', time: '2m ago', rows: '1.2M' },
        { name: 'dim_users', status: 'success', time: '14m ago', rows: '450K' },
        { name: 'stg_payments', status: 'fail', time: '1h ago', rows: '0' }
    ];

    const modelList = document.getElementById('model-list');
    if (modelList) {
        modelList.innerHTML = models.map(m => `
            <div class="model-item">
                <div class="model-info">
                    <span class="model-name">${m.name}</span>
                    <span class="model-meta">${m.rows} rows • ${m.time}</span>
                </div>
                <span class="badge ${m.status}">${m.status}</span>
            </div>
        `).join('');
    }

    // Audit Stream (Shared between Dashboard and Audit View)
    const audits = [
        { ts: '2024-05-20 18:42:01', model: 'fct_orders', status: 'SUCCESS', rows: '1,204,552', dur: '452ms', hash: '8f2a1c' },
        { ts: '2024-05-20 18:30:14', model: 'dim_users', status: 'SUCCESS', rows: '450,120', dur: '124ms', hash: 'a1b2c3' },
        { ts: '2024-05-20 17:15:55', model: 'stg_payments', status: 'FAILURE', rows: '0', dur: '12ms', hash: 'd4e5f6' }
    ];

    const auditFull = document.getElementById('audit-body-full');
    if (auditFull) {
        auditFull.innerHTML = audits.map(a => `
            <tr>
                <td>${a.ts}</td>
                <td style="font-weight:600">${a.model}</td>
                <td><span class="badge ${a.status.toLowerCase()}">${a.status}</span></td>
                <td>${a.rows}</td>
                <td>${a.dur}</td>
                <td>${a.hash}</td>
            </tr>
        `).join('');
    }
}
