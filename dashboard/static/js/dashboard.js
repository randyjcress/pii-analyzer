// PII Analysis Dashboard JavaScript

// Global variables
let charts = {};
let currentJobId = null;
let refreshInterval = 600000; // 10 minutes refresh interval
let refreshTimer = null;
let dbPath = null; // Will be determined by the server or URL parameter
let autoRefreshEnabled = true; // Auto-refresh enabled by default

// DOM Elements
const elements = {
    // Main sections
    errorAlert: document.getElementById('errorAlert'),
    errorMessage: document.getElementById('errorMessage'),
    loadingIndicator: document.getElementById('loadingIndicator'),
    dashboardContent: document.getElementById('dashboardContent'),
    
    // Controls
    jobSelector: document.getElementById('jobSelector'),
    refreshButton: document.getElementById('refreshButton'),
    autoRefreshToggle: document.getElementById('autoRefreshToggle'),
    
    // Job information
    jobName: document.getElementById('jobName'),
    jobStatus: document.getElementById('jobStatus'),
    jobStartTime: document.getElementById('jobStartTime'),
    jobLastUpdated: document.getElementById('jobLastUpdated'),
    
    // Time information
    elapsedTime: document.getElementById('elapsedTime'),
    filesPerHour: document.getElementById('filesPerHour'),
    estimatedCompletion: document.getElementById('estimatedCompletion'),
    
    // Progress
    progressBar: document.getElementById('progressBar'),
    totalFiles: document.getElementById('totalFiles'),
    completedFiles: document.getElementById('completedFiles'),
    pendingFiles: document.getElementById('pendingFiles'),
    processingFiles: document.getElementById('processingFiles'),
    errorFiles: document.getElementById('errorFiles'),
    
    // High risk
    highRiskCount: document.getElementById('highRiskCount'),
    executiveSummary: document.getElementById('executiveSummary'),
    highRiskTable: document.getElementById('highRiskTable'),
    highRiskMoreMessage: document.getElementById('highRiskMoreMessage'),
    
    // Tables
    fileTypesTable: document.getElementById('fileTypesTable'),
    entityTypesTable: document.getElementById('entityTypesTable'),
    
    // Error analysis
    errorAnalysisLoading: document.getElementById('errorAnalysisLoading'),
    errorAnalysisContent: document.getElementById('errorAnalysisContent'),
    totalErrorFiles: document.getElementById('totalErrorFiles'),
    errorSamplesAccordion: document.getElementById('errorSamplesAccordion'),
    
    // Footer
    lastUpdated: document.getElementById('lastUpdated')
};

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    // Set up event listeners
    elements.refreshButton.addEventListener('click', refreshDashboard);
    elements.jobSelector.addEventListener('change', changeJob);
    
    // Set up auto-refresh toggle if it exists
    if (elements.autoRefreshToggle) {
        elements.autoRefreshToggle.checked = autoRefreshEnabled;
        elements.autoRefreshToggle.addEventListener('change', toggleAutoRefresh);
        
        // Update the label to show current refresh interval
        const minutes = Math.floor(refreshInterval / 60000);
        const autoRefreshLabel = document.getElementById('autoRefreshLabel');
        if (autoRefreshLabel) {
            autoRefreshLabel.textContent = `Auto-refresh (${minutes} min)`;
        }
    }
    
    // First, check for URL parameter
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('db_path')) {
        dbPath = urlParams.get('db_path');
    }
    
    // If no URL parameter, fetch server config to get the database path
    if (!dbPath) {
        fetch('/api/config')
            .then(response => response.json())
            .then(data => {
                if (data.db_path) {
                    dbPath = data.db_path;
                } else {
                    dbPath = 'pii_results.db'; // Default as last resort
                }
                loadJobs();
                loadDashboardData();
            })
            .catch(error => {
                console.error('Failed to get server config:', error);
                dbPath = 'pii_results.db'; // Fall back to default
                loadJobs();
                loadDashboardData();
            });
    } else {
        loadJobs();
        loadDashboardData();
    }
    
    // Set up auto-refresh
    if (autoRefreshEnabled) {
        startRefreshTimer();
    }
});

// Toggle auto-refresh
function toggleAutoRefresh() {
    autoRefreshEnabled = elements.autoRefreshToggle.checked;
    
    if (autoRefreshEnabled) {
        startRefreshTimer();
        console.log(`Auto-refresh enabled with interval of ${refreshInterval/1000} seconds`);
    } else {
        stopRefreshTimer();
        console.log('Auto-refresh disabled');
    }
}

// Load available jobs
function loadJobs() {
    let url = '/api/jobs';
    if (dbPath) {
        url += `?db_path=${encodeURIComponent(dbPath)}`;
    }
    
    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error('Failed to load jobs');
            }
            return response.json();
        })
        .then(data => {
            if (data.status === 'success') {
                populateJobSelector(data.jobs);
            } else {
                showError(data.error || 'Failed to load jobs');
            }
        })
        .catch(error => {
            showError(error.message);
        });
}

// Populate job selector dropdown
function populateJobSelector(jobs) {
    elements.jobSelector.innerHTML = '';
    
    if (jobs.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'No jobs available';
        elements.jobSelector.appendChild(option);
        return;
    }
    
    jobs.forEach(job => {
        const option = document.createElement('option');
        option.value = job.job_id;
        option.textContent = `${job.name || 'Job'} (ID: ${job.job_id})`;
        elements.jobSelector.appendChild(option);
    });
    
    // Select first job by default
    elements.jobSelector.value = jobs[0].job_id;
    currentJobId = jobs[0].job_id;
}

// Change selected job
function changeJob() {
    const jobId = elements.jobSelector.value;
    if (jobId !== currentJobId) {
        currentJobId = jobId;
        loadDashboardData(true);
    }
}

// Load dashboard data
function loadDashboardData(forceRefresh = false) {
    // Show loading indicator
    elements.loadingIndicator.classList.remove('d-none');
    elements.dashboardContent.classList.add('d-none');
    elements.errorAlert.classList.add('d-none');
    
    // Build URL with parameters
    let url = '/api/dashboard';
    const params = [];
    
    if (dbPath) {
        params.push(`db_path=${encodeURIComponent(dbPath)}`);
    }
    
    if (currentJobId) {
        params.push(`job_id=${currentJobId}`);
    }
    
    if (forceRefresh) {
        params.push('refresh=1');
    }
    
    if (params.length > 0) {
        url += '?' + params.join('&');
    }
    
    // Fetch dashboard data
    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error('Failed to load dashboard data');
            }
            return response.json();
        })
        .then(data => {
            if (data.status === 'success') {
                updateDashboard(data);
                elements.loadingIndicator.classList.add('d-none');
                elements.dashboardContent.classList.remove('d-none');
            } else {
                showError(data.error || 'Failed to load dashboard data');
            }
        })
        .catch(error => {
            showError(error.message);
        });
}

// Update dashboard with new data
function updateDashboard(data) {
    // Update job information
    elements.jobName.textContent = data.job.name;
    elements.jobStatus.textContent = data.job.status;
    elements.jobStartTime.textContent = data.job.start_time;
    elements.jobLastUpdated.textContent = data.job.last_updated;
    
    // Update time information
    elements.elapsedTime.textContent = data.time.elapsed;
    elements.filesPerHour.textContent = Math.round(data.time.files_per_hour);
    elements.estimatedCompletion.textContent = data.time.estimated_completion;
    
    // Update progress
    elements.progressBar.style.width = `${data.processing.progress_percent}%`;
    elements.progressBar.textContent = `${Math.round(data.processing.progress_percent)}%`;
    elements.totalFiles.textContent = data.processing.total_files.toLocaleString();
    elements.completedFiles.textContent = data.processing.completed.toLocaleString();
    elements.pendingFiles.textContent = data.processing.pending.toLocaleString();
    elements.processingFiles.textContent = data.processing.processing.toLocaleString();
    elements.errorFiles.textContent = data.processing.error.toLocaleString();
    
    // Update file types
    updateFileTypesTable(data.file_types);
    updateFileTypesChart(data.file_types);
    
    // Update entity types
    updateEntityTypesTable(data.entity_types);
    updateEntityTypesChart(data.entity_types);
    
    // Update high risk files
    elements.highRiskCount.textContent = data.high_risk.count.toLocaleString();
    elements.executiveSummary.textContent = data.executive_summary;
    updateHighRiskTable(data.high_risk.files);
    
    // Show "more" message if needed
    elements.highRiskMoreMessage.classList.toggle('d-none', !data.high_risk.has_more);
    
    // Update last updated time
    elements.lastUpdated.textContent = data.updated_at;
    
    // Load error analysis after main dashboard is loaded
    loadErrorAnalysis();
}

// Update file types table
function updateFileTypesTable(fileTypes) {
    elements.fileTypesTable.innerHTML = '';
    
    const totalFiles = fileTypes.reduce((total, type) => total + type.count, 0);
    
    fileTypes.forEach(type => {
        const row = document.createElement('tr');
        
        const typeCell = document.createElement('td');
        typeCell.textContent = type.type || 'Unknown';
        
        const countCell = document.createElement('td');
        countCell.textContent = type.count.toLocaleString();
        
        const percentCell = document.createElement('td');
        const percent = (type.count / totalFiles * 100).toFixed(1);
        percentCell.textContent = `${percent}%`;
        
        row.appendChild(typeCell);
        row.appendChild(countCell);
        row.appendChild(percentCell);
        
        elements.fileTypesTable.appendChild(row);
    });
}

// Update file types chart
function updateFileTypesChart(fileTypes) {
    const ctx = document.getElementById('fileTypesChart').getContext('2d');
    
    // Prepare data
    const labels = [];
    const data = [];
    const colors = [];
    const colorPalette = [
        '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
        '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
    ];
    
    // Sort by count for better visualization
    fileTypes.sort((a, b) => b.count - a.count);
    
    // Take top 9 file types and group the rest as "Other"
    let otherCount = 0;
    
    fileTypes.forEach((type, index) => {
        if (index < 9) {
            labels.push(type.type || 'Unknown');
            data.push(type.count);
            colors.push(colorPalette[index % colorPalette.length]);
        } else {
            otherCount += type.count;
        }
    });
    
    // Add "Other" category if needed
    if (otherCount > 0) {
        labels.push('Other');
        data.push(otherCount);
        colors.push('#858796');
    }
    
    // Destroy existing chart if it exists
    if (charts.fileTypes) {
        charts.fileTypes.destroy();
    }
    
    // Create chart
    charts.fileTypes = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors,
                hoverBackgroundColor: colors.map(color => lightenColor(color, 10)),
                hoverBorderColor: "rgba(234, 236, 244, 1)",
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    backgroundColor: "rgb(0, 0, 0, 0.8)",
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 14
                    },
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = Math.round((value / total) * 100);
                            return `${label}: ${value.toLocaleString()} (${percentage}%)`;
                        }
                    }
                }
            },
            cutout: '60%',
            borderWidth: 0
        }
    });
}

// Update entity types table
function updateEntityTypesTable(entityTypes) {
    elements.entityTypesTable.innerHTML = '';
    
    entityTypes.forEach(type => {
        const row = document.createElement('tr');
        
        const typeCell = document.createElement('td');
        typeCell.textContent = type.display_name || type.type;
        
        const countCell = document.createElement('td');
        countCell.textContent = type.count.toLocaleString();
        
        row.appendChild(typeCell);
        row.appendChild(countCell);
        
        elements.entityTypesTable.appendChild(row);
    });
}

// Update entity types chart
function updateEntityTypesChart(entityTypes) {
    const ctx = document.getElementById('entityTypesChart').getContext('2d');
    
    // Prepare data
    const labels = [];
    const data = [];
    const colors = [
        '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
        '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
    ];
    
    // Sort by count for better visualization
    entityTypes.sort((a, b) => b.count - a.count);
    
    // Take top 10 entity types
    const topEntities = entityTypes.slice(0, 10);
    
    topEntities.forEach((type, index) => {
        labels.push(type.display_name || type.type);
        data.push(type.count);
    });
    
    // Destroy existing chart if it exists
    if (charts.entityTypes) {
        charts.entityTypes.destroy();
    }
    
    // Create chart
    charts.entityTypes = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Entity Count',
                data: data,
                backgroundColor: colors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: "rgb(0, 0, 0, 0.8)",
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 14
                    }
                }
            }
        }
    });
}

// Update high risk files table
function updateHighRiskTable(files) {
    elements.highRiskTable.innerHTML = '';
    
    files.forEach((file, index) => {
        const row = document.createElement('tr');
        
        const numberCell = document.createElement('td');
        numberCell.textContent = index + 1;
        
        const fileCell = document.createElement('td');
        fileCell.textContent = file;
        
        row.appendChild(numberCell);
        row.appendChild(fileCell);
        
        elements.highRiskTable.appendChild(row);
    });
}

// Load error analysis data
function loadErrorAnalysis() {
    // Show loading indicator
    elements.errorAnalysisLoading.classList.remove('d-none');
    elements.errorAnalysisContent.classList.add('d-none');
    
    // Fetch error analysis data
    let url = '/api/error_analysis';
    if (dbPath) {
        url += `?db_path=${encodeURIComponent(dbPath)}`;
    }
    
    fetch(url)
        .then(response => {
            if (!response.ok) {
                throw new Error('Failed to load error analysis');
            }
            return response.json();
        })
        .then(data => {
            if (data.status === 'success') {
                updateErrorAnalysis(data.error_analysis);
                elements.errorAnalysisLoading.classList.add('d-none');
                elements.errorAnalysisContent.classList.remove('d-none');
            } else {
                console.error('Error analysis failed:', data.error);
                elements.errorAnalysisLoading.innerHTML = `<div class="alert alert-danger">Failed to load error analysis: ${data.error}</div>`;
            }
        })
        .catch(error => {
            console.error('Error loading error analysis:', error);
            elements.errorAnalysisLoading.innerHTML = `<div class="alert alert-danger">Failed to load error analysis: ${error.message}</div>`;
        });
}

// Update error analysis section
function updateErrorAnalysis(data) {
    // Update total error count
    elements.totalErrorFiles.textContent = data.total_errors.toLocaleString();
    
    // Update error categories chart
    updateErrorCategoriesChart(data.categories);
    
    // Update error extensions chart
    updateErrorExtensionsChart(data.extensions);
    
    // Update error samples accordion
    updateErrorSamplesAccordion(data.samples);
}

// Update error categories chart
function updateErrorCategoriesChart(categories) {
    const ctx = document.getElementById('errorCategoriesChart').getContext('2d');
    
    // Prepare data
    const labels = [];
    const data = [];
    const colors = [
        '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
        '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
    ];
    
    categories.forEach((category, index) => {
        labels.push(category.name);
        data.push(category.count);
    });
    
    // Destroy existing chart if it exists
    if (charts.errorCategories) {
        charts.errorCategories.destroy();
    }
    
    // Create chart
    charts.errorCategories = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors.slice(0, labels.length),
                hoverBackgroundColor: colors.slice(0, labels.length).map(color => lightenColor(color, 10)),
                hoverBorderColor: "rgba(234, 236, 244, 1)",
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    backgroundColor: "rgb(0, 0, 0, 0.8)",
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 14
                    },
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const percentage = Math.round(context.dataset.data[context.dataIndex] / 
                                context.dataset.data.reduce((a, b) => a + b, 0) * 100);
                            return `${label}: ${value.toLocaleString()} (${percentage}%)`;
                        }
                    }
                }
            },
            cutout: '60%',
            borderWidth: 0
        }
    });
}

// Update error extensions chart
function updateErrorExtensionsChart(extensions) {
    const ctx = document.getElementById('errorExtensionsChart').getContext('2d');
    
    // Prepare data
    const labels = [];
    const data = [];
    const colors = [
        '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
        '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
    ];
    
    // Sort by count
    extensions.sort((a, b) => b.count - a.count);
    
    // Take top 10 extensions
    const topExtensions = extensions.slice(0, 10);
    
    topExtensions.forEach((ext, index) => {
        labels.push(ext.extension || 'Unknown');
        data.push(ext.count);
    });
    
    // Destroy existing chart if it exists
    if (charts.errorExtensions) {
        charts.errorExtensions.destroy();
    }
    
    // Create chart
    charts.errorExtensions = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Error Count',
                data: data,
                backgroundColor: colors.slice(0, labels.length),
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        precision: 0
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: "rgb(0, 0, 0, 0.8)",
                    titleFont: {
                        size: 14
                    },
                    bodyFont: {
                        size: 14
                    }
                }
            }
        }
    });
}

// Update error samples accordion
function updateErrorSamplesAccordion(samples) {
    elements.errorSamplesAccordion.innerHTML = '';
    
    let index = 0;
    for (const category in samples) {
        const categoryId = `category-${index}`;
        
        // Create accordion item
        const accordionItem = document.createElement('div');
        accordionItem.className = 'accordion-item';
        
        // Create accordion header
        const accordionHeader = document.createElement('h2');
        accordionHeader.className = 'accordion-header';
        accordionHeader.id = `heading-${categoryId}`;
        
        const accordionButton = document.createElement('button');
        accordionButton.className = 'accordion-button collapsed';
        accordionButton.type = 'button';
        accordionButton.setAttribute('data-bs-toggle', 'collapse');
        accordionButton.setAttribute('data-bs-target', `#collapse-${categoryId}`);
        accordionButton.setAttribute('aria-expanded', 'false');
        accordionButton.setAttribute('aria-controls', `collapse-${categoryId}`);
        accordionButton.textContent = `${category} (${samples[category].length} samples)`;
        
        accordionHeader.appendChild(accordionButton);
        accordionItem.appendChild(accordionHeader);
        
        // Create accordion content
        const accordionCollapse = document.createElement('div');
        accordionCollapse.id = `collapse-${categoryId}`;
        accordionCollapse.className = 'accordion-collapse collapse';
        accordionCollapse.setAttribute('aria-labelledby', `heading-${categoryId}`);
        accordionCollapse.setAttribute('data-bs-parent', '#errorSamplesAccordion');
        
        const accordionBody = document.createElement('div');
        accordionBody.className = 'accordion-body';
        
        // Create sample list
        const sampleList = document.createElement('ul');
        sampleList.className = 'list-group';
        
        samples[category].forEach(sample => {
            const listItem = document.createElement('li');
            listItem.className = 'list-group-item';
            
            const filePath = document.createElement('div');
            filePath.className = 'fw-bold';
            filePath.textContent = sample.file_path;
            
            const errorMessage = document.createElement('div');
            errorMessage.className = 'text-danger mt-2';
            errorMessage.textContent = sample.error || 'No error message available';
            
            listItem.appendChild(filePath);
            listItem.appendChild(errorMessage);
            sampleList.appendChild(listItem);
        });
        
        accordionBody.appendChild(sampleList);
        accordionCollapse.appendChild(accordionBody);
        accordionItem.appendChild(accordionCollapse);
        
        elements.errorSamplesAccordion.appendChild(accordionItem);
        index++;
    }
}

// Show error message
function showError(message) {
    elements.errorMessage.textContent = message;
    elements.errorAlert.classList.remove('d-none');
    elements.loadingIndicator.classList.add('d-none');
    elements.dashboardContent.classList.add('d-none');
}

// Refresh dashboard
function refreshDashboard() {
    loadDashboardData(true);
    resetRefreshTimer();
}

// Start refresh timer
function startRefreshTimer() {
    if (refreshTimer) {
        clearTimeout(refreshTimer);
    }
    
    if (autoRefreshEnabled) {
        refreshTimer = setTimeout(function() {
            loadDashboardData();
            startRefreshTimer();
        }, refreshInterval);
    }
}

// Stop refresh timer
function stopRefreshTimer() {
    if (refreshTimer) {
        clearTimeout(refreshTimer);
        refreshTimer = null;
    }
}

// Reset refresh timer
function resetRefreshTimer() {
    if (autoRefreshEnabled) {
        stopRefreshTimer();
        startRefreshTimer();
    }
}

// Utility: Lighten a color by percentage
function lightenColor(color, percent) {
    const num = parseInt(color.slice(1), 16);
    const amt = Math.round(2.55 * percent);
    const R = (num >> 16) + amt;
    const G = (num >> 8 & 0x00FF) + amt;
    const B = (num & 0x0000FF) + amt;
    
    return '#' + (
        0x1000000 +
        (R < 255 ? R < 1 ? 0 : R : 255) * 0x10000 +
        (G < 255 ? G < 1 ? 0 : G : 255) * 0x100 +
        (B < 255 ? B < 1 ? 0 : B : 255)
    ).toString(16).slice(1);
} 