/**
 * Audio Player for Super Gems Podcast
 */
class SuperGemsPodcastPlayer {
    constructor() {
        this.audio = null;
        this.currentAudio = null;
        this.isPlaying = false;
        this.duration = 0;
        this.currentTime = 0;
        this.volume = 0.8;
        
        this.initializePlayer();
        this.loadLatestAudio();
    }
    
    initializePlayer() {
        // Create player container
        const playerContainer = document.createElement('div');
        playerContainer.className = 'podcast-player';
        playerContainer.innerHTML = `
            <div class="player-header">
                <h3>🎧 HN Super Gems Podcast</h3>
                <div class="player-status">No audio loaded</div>
            </div>
            <div class="player-controls">
                <button class="btn-play-pause" disabled>⏵️</button>
                <div class="progress-container">
                    <div class="progress-bar">
                        <div class="progress-fill"></div>
                    </div>
                    <div class="time-display">
                        <span class="current-time">0:00</span> / 
                        <span class="total-time">0:00</span>
                    </div>
                </div>
                <div class="volume-container">
                    <button class="btn-volume">🔊</button>
                    <input type="range" class="volume-slider" min="0" max="1" step="0.1" value="0.8">
                </div>
                <button class="btn-download" disabled>⬇️ Download</button>
            </div>
            <div class="player-info">
                <div class="audio-metadata"></div>
            </div>
        `;
        
        // Add player to page (insert after header or at top of content)
        const header = document.querySelector('.header') || document.querySelector('header');
        if (header) {
            header.insertAdjacentElement('afterend', playerContainer);
        } else {
            document.body.insertBefore(playerContainer, document.body.firstChild);
        }
        
        this.playerContainer = playerContainer;
        this.bindEvents();
        this.addStyles();
    }
    
    addStyles() {
        if (document.getElementById('podcast-player-styles')) return;
        
        const styles = `
            <style id="podcast-player-styles">
                .podcast-player {
                    background: #fff;
                    border: 1px solid #e0e0e0;
                    margin: 10px auto;
                    max-width: 1200px;
                    padding: 15px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }
                
                .player-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 15px;
                    border-bottom: 1px solid #f0f0f0;
                    padding-bottom: 10px;
                }
                
                .player-header h3 {
                    margin: 0;
                    color: #ff6600;
                    font-size: 14pt;
                }
                
                .player-status {
                    font-size: 9pt;
                    color: #666;
                }
                
                .player-controls {
                    display: flex;
                    align-items: center;
                    gap: 15px;
                    margin-bottom: 10px;
                }
                
                .btn-play-pause {
                    background: #ff6600;
                    color: white;
                    border: none;
                    border-radius: 50%;
                    width: 50px;
                    height: 50px;
                    font-size: 16px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                
                .btn-play-pause:disabled {
                    background: #ccc;
                    cursor: not-allowed;
                }
                
                .btn-play-pause:hover:not(:disabled) {
                    background: #e55a00;
                }
                
                .progress-container {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    gap: 5px;
                }
                
                .progress-bar {
                    width: 100%;
                    height: 6px;
                    background: #f0f0f0;
                    border-radius: 3px;
                    cursor: pointer;
                    position: relative;
                }
                
                .progress-fill {
                    height: 100%;
                    background: #ff6600;
                    border-radius: 3px;
                    width: 0%;
                    transition: width 0.1s ease;
                }
                
                .time-display {
                    font-size: 8pt;
                    color: #666;
                    text-align: center;
                }
                
                .volume-container {
                    display: flex;
                    align-items: center;
                    gap: 5px;
                }
                
                .btn-volume {
                    background: none;
                    border: none;
                    font-size: 14px;
                    cursor: pointer;
                }
                
                .volume-slider {
                    width: 60px;
                }
                
                .btn-download {
                    background: #28a745;
                    color: white;
                    border: none;
                    padding: 8px 12px;
                    border-radius: 3px;
                    cursor: pointer;
                    font-size: 8pt;
                }
                
                .btn-download:disabled {
                    background: #ccc;
                    cursor: not-allowed;
                }
                
                .btn-download:hover:not(:disabled) {
                    background: #218838;
                }
                
                .player-info {
                    font-size: 8pt;
                    color: #666;
                    margin-top: 10px;
                }
                
                .audio-metadata {
                    display: flex;
                    gap: 20px;
                    flex-wrap: wrap;
                }
                
                .metadata-item {
                    display: flex;
                    align-items: center;
                    gap: 5px;
                }
                
                @media (max-width: 768px) {
                    .player-controls {
                        flex-wrap: wrap;
                        gap: 10px;
                    }
                    
                    .progress-container {
                        order: 3;
                        width: 100%;
                    }
                    
                    .audio-metadata {
                        flex-direction: column;
                        gap: 5px;
                    }
                }
            </style>
        `;
        
        document.head.insertAdjacentHTML('beforeend', styles);
    }
    
    bindEvents() {
        const playPauseBtn = this.playerContainer.querySelector('.btn-play-pause');
        const progressBar = this.playerContainer.querySelector('.progress-bar');
        const volumeSlider = this.playerContainer.querySelector('.volume-slider');
        const volumeBtn = this.playerContainer.querySelector('.btn-volume');
        const downloadBtn = this.playerContainer.querySelector('.btn-download');
        
        playPauseBtn.addEventListener('click', () => this.togglePlayPause());
        progressBar.addEventListener('click', (e) => this.seekTo(e));
        volumeSlider.addEventListener('input', (e) => this.setVolume(e.target.value));
        volumeBtn.addEventListener('click', () => this.toggleMute());
        downloadBtn.addEventListener('click', () => this.downloadAudio());
        
        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
            
            switch(e.key) {
                case ' ':
                    e.preventDefault();
                    this.togglePlayPause();
                    break;
                case 'ArrowLeft':
                    e.preventDefault();
                    this.seek(-10);
                    break;
                case 'ArrowRight':
                    e.preventDefault();
                    this.seek(10);
                    break;
            }
        });
    }
    
    async loadLatestAudio() {
        // Detect if we're on a static file served by nginx (like super-gems.html)
        const isStaticFile = window.location.pathname.endsWith('.html') || !window.location.pathname.includes('/api/');
        
        if (isStaticFile) {
            // For static files, use direct nginx redirect to latest.mp3
            this.loadStaticAudio();
        } else {
            // For dynamic Flask routes, use the API
            this.loadApiAudio();
        }
    }
    
    loadStaticAudio() {
        try {
            // Simple metadata for static mode
            const metadata = {
                filename: 'latest.mp3',
                gems_count: 9,
                estimated_duration_minutes: 17,
                generation_timestamp: new Date().toISOString(),
                file_size_bytes: 7400000  // Approximate
            };
            
            const streamUrl = '/latest.mp3';  // nginx will redirect this
            const downloadUrl = '/latest.mp3';
            
            this.loadAudio(metadata, streamUrl, downloadUrl);
            this.updateStatus('Audio loaded (static mode)');
            
        } catch (error) {
            console.error('Error loading static audio:', error);
            this.updateStatus('Failed to load audio');
        }
    }
    
    async loadApiAudio() {
        try {
            const response = await fetch('/api/audio/super-gems/latest');
            if (!response.ok) {
                if (response.status === 404) {
                    this.updateStatus('No podcast audio available yet');
                    return;
                }
                throw new Error(`HTTP ${response.status}`);
            }
            
            const data = await response.json();
            this.loadAudio(data.audio, data.stream_url, data.download_url);
            
        } catch (error) {
            console.error('Error loading latest audio:', error);
            this.updateStatus('Failed to load audio');
        }
    }
    
    loadAudio(metadata, streamUrl, downloadUrl) {
        // Clean up existing audio
        if (this.audio) {
            this.audio.pause();
            this.audio.removeEventListener('loadedmetadata', this.onLoadedMetadata);
            this.audio.removeEventListener('timeupdate', this.onTimeUpdate);
            this.audio.removeEventListener('ended', this.onEnded);
            this.audio.removeEventListener('error', this.onError);
        }
        
        // Create new audio element
        this.audio = new Audio(streamUrl);
        this.audio.volume = this.volume;
        this.currentAudio = {
            metadata,
            streamUrl,
            downloadUrl
        };
        
        // Bind audio events
        this.onLoadedMetadata = () => {
            this.duration = this.audio.duration;
            this.updateTimeDisplay();
            this.enableControls();
        };
        
        this.onTimeUpdate = () => {
            this.currentTime = this.audio.currentTime;
            this.updateProgress();
            this.updateTimeDisplay();
        };
        
        this.onEnded = () => {
            this.isPlaying = false;
            this.updatePlayPauseButton();
        };
        
        this.onError = (e) => {
            console.error('Audio error:', e);
            this.updateStatus('Error loading audio');
        };
        
        this.audio.addEventListener('loadedmetadata', this.onLoadedMetadata);
        this.audio.addEventListener('timeupdate', this.onTimeUpdate);
        this.audio.addEventListener('ended', this.onEnded);
        this.audio.addEventListener('error', this.onError);
        
        // Update UI
        this.updateStatus('Audio loaded');
        this.updateMetadata(metadata);
        this.playerContainer.querySelector('.btn-download').disabled = false;
        
        // Load audio metadata
        this.audio.load();
    }
    
    togglePlayPause() {
        if (!this.audio) return;
        
        if (this.isPlaying) {
            this.audio.pause();
            this.isPlaying = false;
        } else {
            this.audio.play()
                .then(() => {
                    this.isPlaying = true;
                })
                .catch(error => {
                    console.error('Error playing audio:', error);
                    this.updateStatus('Error playing audio');
                });
        }
        
        this.updatePlayPauseButton();
    }
    
    seekTo(event) {
        if (!this.audio || !this.duration) return;
        
        const progressBar = event.currentTarget;
        const rect = progressBar.getBoundingClientRect();
        const clickX = event.clientX - rect.left;
        const percentage = clickX / rect.width;
        const newTime = percentage * this.duration;
        
        this.audio.currentTime = newTime;
    }
    
    seek(seconds) {
        if (!this.audio) return;
        
        const newTime = Math.max(0, Math.min(this.duration, this.audio.currentTime + seconds));
        this.audio.currentTime = newTime;
    }
    
    setVolume(value) {
        this.volume = parseFloat(value);
        if (this.audio) {
            this.audio.volume = this.volume;
        }
        
        // Update volume button icon
        const volumeBtn = this.playerContainer.querySelector('.btn-volume');
        if (this.volume === 0) {
            volumeBtn.textContent = '🔇';
        } else if (this.volume < 0.5) {
            volumeBtn.textContent = '🔉';
        } else {
            volumeBtn.textContent = '🔊';
        }
    }
    
    toggleMute() {
        if (this.volume > 0) {
            this.previousVolume = this.volume;
            this.setVolume(0);
            this.playerContainer.querySelector('.volume-slider').value = 0;
        } else {
            const restoreVolume = this.previousVolume || 0.8;
            this.setVolume(restoreVolume);
            this.playerContainer.querySelector('.volume-slider').value = restoreVolume;
        }
    }
    
    downloadAudio() {
        if (!this.currentAudio) return;
        
        const link = document.createElement('a');
        link.href = this.currentAudio.downloadUrl;
        link.download = this.currentAudio.metadata.filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    
    updatePlayPauseButton() {
        const btn = this.playerContainer.querySelector('.btn-play-pause');
        btn.textContent = this.isPlaying ? '⏸️' : '⏵️';
    }
    
    updateProgress() {
        if (!this.duration) return;
        
        const percentage = (this.currentTime / this.duration) * 100;
        const progressFill = this.playerContainer.querySelector('.progress-fill');
        progressFill.style.width = `${percentage}%`;
    }
    
    updateTimeDisplay() {
        const currentTimeSpan = this.playerContainer.querySelector('.current-time');
        const totalTimeSpan = this.playerContainer.querySelector('.total-time');
        
        currentTimeSpan.textContent = this.formatTime(this.currentTime);
        totalTimeSpan.textContent = this.formatTime(this.duration);
    }
    
    updateStatus(status) {
        const statusEl = this.playerContainer.querySelector('.player-status');
        statusEl.textContent = status;
    }
    
    updateMetadata(metadata) {
        const metadataContainer = this.playerContainer.querySelector('.audio-metadata');
        
        const items = [];
        
        if (metadata.gems_count) {
            items.push(`${metadata.gems_count} gems`);
        }
        
        if (metadata.estimated_duration_minutes) {
            items.push(`~${metadata.estimated_duration_minutes} min`);
        }
        
        if (metadata.generation_timestamp) {
            const date = new Date(metadata.generation_timestamp);
            items.push(`Generated ${date.toLocaleDateString()}`);
        }
        
        if (metadata.file_size_bytes) {
            const sizeMB = (metadata.file_size_bytes / (1024 * 1024)).toFixed(1);
            items.push(`${sizeMB} MB`);
        }
        
        metadataContainer.innerHTML = items.map(item => 
            `<span class="metadata-item">${item}</span>`
        ).join('');
    }
    
    enableControls() {
        this.playerContainer.querySelector('.btn-play-pause').disabled = false;
    }
    
    formatTime(seconds) {
        if (!seconds || isNaN(seconds)) return '0:00';
        
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = Math.floor(seconds % 60);
        return `${minutes}:${remainingSeconds.toString().padStart(2, '0')}`;
    }
}

// Auto-initialize on super gems pages
document.addEventListener('DOMContentLoaded', () => {
    // Check if we're on super gems page or if podcast player should be shown
    const currentPath = window.location.pathname;
    const shouldShowPlayer = currentPath.includes('super-gems') || 
                           document.querySelector('.gem') || // Super gems content present
                           document.body.dataset.showPodcastPlayer === 'true';
    
    if (shouldShowPlayer) {
        window.podcastPlayer = new SuperGemsPodcastPlayer();
    }
});

// Export for manual initialization
window.SuperGemsPodcastPlayer = SuperGemsPodcastPlayer;