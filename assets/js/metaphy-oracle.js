/* ═══════════════════════════════════════════════════════════════════════
   metaphy-oracle.js — The Oracle Voice AI Module
   Shared component · Metaphysics and Computing
   ═══════════════════════════════════════════════════════════════════════
   Usage:
     <!-- Optional per-page config (before script) -->
     <script>
       window.METAPHY_ORACLE_CONFIG = { pageContext: 'About Us page' };
     </script>
     <script src="/assets/js/metaphy-oracle.js"></script>
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  class Oracle {
    constructor(config = {}) {
      this.config = {
        restUrl: config.restUrl || 'https://metaphysics-and-computing-production.up.railway.app/api/chat',
        healthUrl: config.healthUrl || 'https://metaphysics-and-computing-production.up.railway.app/api/health',
        wsUrl: config.wsUrl || null,
        maxMessageLength: 1000,
        sessionTimeout: 30 * 60 * 1000,
        enableMockMode: config.enableMockMode !== undefined ? config.enableMockMode : true,
        pageContext: config.pageContext || '',
        ...config
      };

      this.state = 'idle';
      this.panelOpen = false;
      this.sessionId = this._generateSessionId();
      this.messages = [];
      this.audioContext = null;
      this.audioQueue = [];
      this.isPlayingAudio = false;
      this.currentSource = null;
      this.recognition = null;
      this.isRecording = false;
      this.backendAvailable = false;
      this.streamAbort = null;

      // DOM refs (populated by render)
      this.els = {};
    }

    /* ─── SESSION ─── */
    _generateSessionId() {
      return 'oracle_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8);
    }

    /* ─── STATE ─── */
    setState(s) {
      this.state = s;
      const orb = this.els.orb;
      if (!orb) return;
      orb.classList.remove('listening', 'thinking', 'speaking', 'offline', 'panel-open');
      if (s !== 'idle') orb.classList.add(s);
      if (this.panelOpen && s === 'idle') orb.classList.add('panel-open');

      const status = this.els.status;
      if (status) {
        const labels = {
          idle: this.backendAvailable ? 'Connected to The Oracle' : 'Demo mode \u2014 offline',
          listening: 'Listening...',
          thinking: 'Thinking...',
          speaking: 'Speaking...',
          offline: 'Offline \u2014 text only'
        };
        status.textContent = labels[s] || '';
        status.className = 'oracle-status' + (this.backendAvailable ? ' connected' : '');
      }
    }

    /* ─── RENDER ─── */
    render() {
      // Orb
      const orb = document.createElement('div');
      orb.className = 'oracle-orb';
      orb.id = 'oracleOrb';
      orb.setAttribute('aria-label', 'Open The Oracle voice assistant');
      orb.innerHTML = `<svg class="oracle-orb-svg" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
        <!-- Metatron's Cube wireframe -->
        <circle cx="50" cy="50" r="44" stroke="#3D8C8C" stroke-width="0.5" opacity="0.7"/>
        <circle cx="50" cy="50" r="22" stroke="#C9A84C" stroke-width="0.4" opacity="0.5"/>
        <!-- Hexagonal vertices -->
        <circle cx="50" cy="6" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <circle cx="88" cy="28" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <circle cx="88" cy="72" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <circle cx="50" cy="94" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <circle cx="12" cy="72" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <circle cx="12" cy="28" r="1.5" fill="#3D8C8C" opacity="0.8"/>
        <!-- Outer hex -->
        <polygon points="50,6 88,28 88,72 50,94 12,72 12,28" stroke="#3D8C8C" stroke-width="0.5" fill="none" opacity="0.6"/>
        <!-- Inner hex (rotated 30deg) -->
        <polygon points="72,14 94,50 72,86 28,86 6,50 28,14" stroke="#C9A84C" stroke-width="0.3" fill="none" opacity="0.35"/>
        <!-- Cross lines (Metatron connections) -->
        <line x1="50" y1="6" x2="50" y2="94" stroke="#3D8C8C" stroke-width="0.3" opacity="0.3"/>
        <line x1="12" y1="28" x2="88" y2="72" stroke="#3D8C8C" stroke-width="0.3" opacity="0.3"/>
        <line x1="88" y1="28" x2="12" y2="72" stroke="#3D8C8C" stroke-width="0.3" opacity="0.3"/>
        <line x1="50" y1="6" x2="88" y2="72" stroke="#C9A84C" stroke-width="0.2" opacity="0.2"/>
        <line x1="50" y1="6" x2="12" y2="72" stroke="#C9A84C" stroke-width="0.2" opacity="0.2"/>
        <line x1="88" y1="28" x2="50" y2="94" stroke="#C9A84C" stroke-width="0.2" opacity="0.2"/>
        <line x1="12" y1="28" x2="50" y2="94" stroke="#C9A84C" stroke-width="0.2" opacity="0.2"/>
        <line x1="88" y1="28" x2="12" y2="72" stroke="#C9A84C" stroke-width="0.2" opacity="0.15"/>
        <line x1="12" y1="28" x2="88" y2="72" stroke="#C9A84C" stroke-width="0.2" opacity="0.15"/>
        <!-- Center point -->
        <circle cx="50" cy="50" r="2" fill="#C9A84C" opacity="0.6"/>
      </svg>`;
      document.body.appendChild(orb);

      // Tooltip
      const tooltip = document.createElement('div');
      tooltip.className = 'oracle-tooltip';
      tooltip.textContent = 'Ask The Oracle';
      document.body.appendChild(tooltip);

      // Panel
      const panel = document.createElement('div');
      panel.className = 'oracle-panel';
      panel.id = 'oraclePanel';
      panel.innerHTML = `
        <div class="oracle-header">
          <span class="oracle-title">\u2726 THE ORACLE</span>
          <div class="oracle-header-controls">
            <button class="oracle-visualize" aria-label="Visualize in NEUROLUX" title="Open NEUROLUX Neural Map">\u2726</button>
            <button class="oracle-minimize" aria-label="Minimize">\u2500</button>
            <button class="oracle-close" aria-label="Close">\u2715</button>
          </div>
        </div>
        <div class="oracle-messages" id="oracleMessages"></div>
        <div class="oracle-input-area">
          <div class="oracle-waveform" id="oracleWaveform"><canvas id="oracleWaveCanvas"></canvas></div>
          <div class="oracle-input-row">
            <button class="oracle-mic-btn" id="oracleMicBtn" aria-label="Hold to speak">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z"/><path d="M19 10v2a7 7 0 01-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>
            </button>
            <input type="text" class="oracle-text-input" id="oracleTextInput" placeholder="Type or hold mic to speak..." autocomplete="off">
            <button class="oracle-send-btn" id="oracleSendBtn" aria-label="Send">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
            </button>
          </div>
          <div class="oracle-status" id="oracleStatus">Initializing...</div>
        </div>`;
      document.body.appendChild(panel);

      // Cache DOM refs
      this.els = {
        orb, tooltip, panel,
        messages: document.getElementById('oracleMessages'),
        textInput: document.getElementById('oracleTextInput'),
        sendBtn: document.getElementById('oracleSendBtn'),
        micBtn: document.getElementById('oracleMicBtn'),
        status: document.getElementById('oracleStatus'),
        waveform: document.getElementById('oracleWaveform'),
        waveCanvas: document.getElementById('oracleWaveCanvas'),
        minimize: panel.querySelector('.oracle-minimize'),
        close: panel.querySelector('.oracle-close'),
      };

      this._bindEvents();
      this._checkHealth();
      this._initSpeechRecognition();
      this._showFirstVisit();

      // Periodic health re-check
      setInterval(() => this._checkHealth(), 30000);

      // Welcome message
      this._appendMessage('ai', 'Welcome to Metaphysics and Computing. I\u2019m The Oracle \u2014 ask me anything about our research, our philosophy, or just have a conversation. I\u2019m here to help.');
    }

    /* ─── EVENTS ─── */
    _bindEvents() {
      // Orb click
      this.els.orb.addEventListener('click', () => this.togglePanel());

      // Panel controls
      this.els.minimize.addEventListener('click', () => this.togglePanel());
      this.els.close.addEventListener('click', () => this.togglePanel());

      // Visualize in NEUROLUX
      const vizBtn = this.els.panel.querySelector('.oracle-visualize');
      if (vizBtn) {
        vizBtn.addEventListener('click', () => {
          const lastUserMsg = this.messages.filter(m => m.role === 'user').pop();
          const q = lastUserMsg ? encodeURIComponent(lastUserMsg.text) : '';
          const url = q ? `/neurolux/?q=${q}&highlight=true` : '/neurolux/';
          window.open(url, '_blank');
        });
      }

      // Text input
      this.els.textInput.addEventListener('input', () => {
        this.els.sendBtn.classList.toggle('active', this.els.textInput.value.trim().length > 0);
      });
      this.els.textInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          this._sendFromInput();
        }
      });
      this.els.sendBtn.addEventListener('click', () => this._sendFromInput());

      // Mic (push-to-talk)
      const micDown = () => { if (!this.els.micBtn.classList.contains('unsupported')) this.startListening(); };
      const micUp = () => { if (this.isRecording) this.stopListening(); };
      this.els.micBtn.addEventListener('mousedown', micDown);
      this.els.micBtn.addEventListener('touchstart', (e) => { e.preventDefault(); micDown(); });
      document.addEventListener('mouseup', micUp);
      document.addEventListener('touchend', micUp);

      // Keyboard shortcuts
      document.addEventListener('keydown', (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
          e.preventDefault();
          this.togglePanel();
        }
        if (e.key === 'Escape' && this.panelOpen) {
          this.togglePanel();
        }
      });

      // Close bubble on scroll
      window.addEventListener('scroll', () => {
        const bubble = document.querySelector('.oracle-bubble');
        if (bubble) bubble.remove();
      }, { once: true });
    }

    /* ─── PANEL ─── */
    togglePanel() {
      this.panelOpen = !this.panelOpen;
      this.els.panel.classList.toggle('open', this.panelOpen);
      this.els.orb.classList.toggle('panel-open', this.panelOpen);

      // Remove first-visit bubble
      const bubble = document.querySelector('.oracle-bubble');
      if (bubble) bubble.remove();

      if (this.panelOpen) {
        // Init AudioContext on first user gesture
        if (!this.audioContext) this._initAudioContext();
        setTimeout(() => this.els.textInput.focus(), 350);
        this.els.messages.scrollTop = this.els.messages.scrollHeight;
      } else {
        this.stopAudio();
        if (this.isRecording) this.stopListening();
        this.setState('idle');
      }
    }

    /* ─── SEND MESSAGE ─── */
    _sendFromInput() {
      const text = this.els.textInput.value.trim();
      if (!text || this.state === 'thinking' || this.state === 'speaking') return;
      this.els.textInput.value = '';
      this.els.sendBtn.classList.remove('active');
      this.sendMessage(text);
    }

    async sendMessage(text) {
      if (!text || text.length > this.config.maxMessageLength) return;

      this._appendMessage('user', text);
      this.stopAudio();

      // Try REST streaming first, then mock fallback
      if (this.backendAvailable) {
        await this._sendREST(text);
      } else if (this.config.enableMockMode) {
        await this._sendMock(text);
      } else {
        this._appendMessage('ai', 'The Oracle is currently offline. Please try again later, or reach out directly at Logan@MetaphysicsandComputing.com.');
      }
    }

    /* ─── REST NDJSON STREAMING ─── */
    async _sendREST(text) {
      this.setState('thinking');
      const thinkingEl = this._showThinking();

      if (this.streamAbort) this.streamAbort.abort();
      this.streamAbort = new AbortController();

      try {
        const res = await fetch(this.config.restUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Accept': 'application/x-ndjson' },
          body: JSON.stringify({
            message: text,
            sessionId: this.sessionId,
            page: this.config.pageContext || document.title
          }),
          signal: this.streamAbort.signal
        });

        if (!res.ok) throw new Error('HTTP ' + res.status);

        thinkingEl.remove();
        let msgEl = null;
        let fullText = '';
        let unspokenText = '';

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { value, done } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop();

          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const data = JSON.parse(line);

              if (data.type === 'text') {
                if (!msgEl) {
                  msgEl = this._appendMessage('ai', '', true);
                }
                fullText += data.content + ' ';
                unspokenText += data.content + ' ';
                this._streamText(msgEl, data.content + ' ');
                // Don't setState('speaking') here — let audio playback
                // drive the orb pulse so visuals sync with actual voice
              }

              if (data.type === 'audio' && data.data) {
                this._queueAudio(data.data);
                unspokenText = '';
              }

              if (data.type === 'tts_error') {
                if (unspokenText.trim()) {
                  this._speakFallback(unspokenText.trim());
                  unspokenText = '';
                }
              }

              if (data.type === 'done') {
                if (unspokenText.trim()) {
                  this._speakFallback(unspokenText.trim());
                  unspokenText = '';
                }
                this._finalizeStream(msgEl);
              }
            } catch (e) { /* skip malformed lines */ }
          }
        }

        if (unspokenText.trim()) this._speakFallback(unspokenText.trim());
        if (msgEl) this._finalizeStream(msgEl);
        if (!fullText) {
          this._appendMessage('ai', 'I received your message but had trouble forming a response. Please try again.');
        }

      } catch (err) {
        if (err.name === 'AbortError') return;
        thinkingEl.remove();
        console.warn('Oracle REST error:', err);
        // Fallback to mock
        if (this.config.enableMockMode) {
          this.backendAvailable = false;
          await this._sendMock(text);
        } else {
          this._appendMessage('ai', 'Connection lost. Please try again in a moment.');
          this.setState('idle');
        }
      }
    }

    _showThinking() {
      const el = document.createElement('div');
      el.className = 'oracle-msg oracle-msg-ai';
      el.innerHTML = '<div class="oracle-msg-indicator"></div><div class="oracle-thinking"><span></span><span></span><span></span></div>';
      this.els.messages.appendChild(el);
      this.els.messages.scrollTop = this.els.messages.scrollHeight;
      return el;
    }

    _streamText(msgEl, text) {
      const content = msgEl.querySelector('.oracle-msg-content');
      // Remove cursor if present
      const cursor = content.querySelector('.oracle-cursor');
      if (cursor) cursor.remove();
      // Append text characters
      content.insertAdjacentText('beforeend', text);
      // Re-add cursor
      const c = document.createElement('span');
      c.className = 'oracle-cursor';
      content.appendChild(c);
      this.els.messages.scrollTop = this.els.messages.scrollHeight;
    }

    _finalizeStream(msgEl) {
      if (!msgEl) return;
      const cursor = msgEl.querySelector('.oracle-cursor');
      if (cursor) cursor.remove();
      if (!this.isPlayingAudio) this.setState('idle');
    }

    /* ─── MOCK MODE (Offline Demo) ─── */
    async _sendMock(text) {
      this.setState('thinking');
      const thinkingEl = this._showThinking();

      // Simulate thinking delay
      await new Promise(r => setTimeout(r, 800 + Math.random() * 600));
      thinkingEl.remove();

      const response = this._getMockResponse(text);
      const msgEl = this._appendMessage('ai', '', true);

      // Stream text word by word
      const words = response.split(' ');
      for (let i = 0; i < words.length; i++) {
        const word = words[i] + (i < words.length - 1 ? ' ' : '');
        this._streamText(msgEl, word);
        await new Promise(r => setTimeout(r, 30 + Math.random() * 30));
      }

      this._finalizeStream(msgEl);

      // Use browser TTS as fallback voice
      this._speakFallback(response);
    }

    _getMockResponse(text) {
      const q = text.toLowerCase();
      const responses = {
        qegg: 'QEGG \u2014 Quantum Entangled Geometric Grid \u2014 is one of the foundational theoretical frameworks developed by Metaphy LLC. It explores the fascinating idea that quantum entanglement and geometric structure share a common foundation, modeled through dodecahedral QR-morphed pentagons for light-based data transmission with self-replicating swarms and ternary modulation.',
        drgfc: 'DRGFC \u2014 Dodecahedral Recursive Geometric Fractal Compression \u2014 is an advanced data compression system that uses Platonic solids and quantum principles. In benchmarks, it has achieved compression ratios up to 590 times greater than conventional methods, representing a paradigm shift in how we think about data storage and transmission.',
        hmss: 'The Heavenly Morning Star System \u2014 HMSS \u2014 is the integrative platform that combines QEGG, DRGFC, LWIS, SPTS, BPCS, and the 2S1C architecture into a unified metaphysical computing framework. It embodies infinite paths and sacred geometry to unlock unseen truths in the electromagnetic spectrum.',
        quad: 'QUAD \u2014 Quantum Universal Arrayed Domain \u2014 envisions a planetary-scale network of interconnected HMSS units. It represents perhaps the most ambitious aspect of Metaphy\u2019s research: a singular, shared quantum infrastructure that could unify global communications through entanglement.',
        logan: 'Randell Logan Smith \u2014 known as Logan \u2014 is the founder of Metaphy LLC and the visionary behind Metaphysics and Computing. His work sits at the extraordinary intersection of Platonic geometry, quantum physics, and advanced computation, driven by the belief that "just because there is nothing there does not mean there is not a thing there."',
        metaphy: 'Metaphy LLC is a research studio exploring how timeless geometry and modern computation unlock new ways to sense, encode, and share meaning. We prototype light-based systems, novel encodings, and humane tooling that make patterns visible, intent operable, and knowledge verifiable \u2014 without trading away privacy or wonder.',
        mission: 'Our mission: In the Heavenly Morning Star System, for the maximum benefit of life, Metaphy LLC stands as the celestial forge where metaphysics and computing converge in divine alchemy. We boldly unravel the veiled tapestry of existence, awakening humanity to the unseen symphonies of the universe.',
        hello: 'Hello! Welcome to Metaphysics and Computing. I\u2019m here to discuss our research into quantum geometry, advanced compression, and metaphysical computing frameworks. What would you like to explore?',
        hi: 'Hello! Welcome to Metaphysics and Computing. I\u2019m here to discuss our research into quantum geometry, advanced compression, and metaphysical computing frameworks. What would you like to explore?',
        help: 'I can tell you about our research projects (QEGG, DRGFC, HMSS, QUAD), our company philosophy, our founder Logan Smith, or our mission and core beliefs. I can also discuss the intersection of metaphysics and computing more broadly. What interests you?'
      };

      for (const [key, val] of Object.entries(responses)) {
        if (q.includes(key)) return val;
      }

      return 'That\u2019s an interesting question. While I can speak to our publicly available research on quantum geometry, compression systems, and metaphysical computing, I may not have specific information on that topic. Feel free to ask about QEGG, DRGFC, HMSS, QUAD, or our company philosophy \u2014 or reach out to Logan directly at Logan@MetaphysicsandComputing.com for deeper conversations.';
    }

    /* ─── AUDIO QUEUE (Web Audio API) ─── */
    _initAudioContext() {
      try {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
      } catch (e) {
        console.warn('Oracle: AudioContext not available');
      }
    }

    _queueAudio(base64Data) {
      this.audioQueue.push(base64Data);
      if (!this.isPlayingAudio) this._playNextAudio();
    }

    async _playNextAudio() {
      if (this.audioQueue.length === 0) {
        this.isPlayingAudio = false;
        if (this.state === 'speaking') this.setState('idle');
        return;
      }
      this.isPlayingAudio = true;
      this.setState('speaking');

      const base64 = this.audioQueue.shift();
      try {
        const binary = atob(base64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

        const audioBuffer = await this.audioContext.decodeAudioData(bytes.buffer);
        const source = this.audioContext.createBufferSource();
        source.buffer = audioBuffer;

        // Crossfade envelope (20ms ramps, skip if clip too short)
        const gain = this.audioContext.createGain();
        const fadeDuration = 0.02;
        if (audioBuffer.duration > fadeDuration * 2) {
          gain.gain.setValueAtTime(0, this.audioContext.currentTime);
          gain.gain.linearRampToValueAtTime(1, this.audioContext.currentTime + fadeDuration);
          gain.gain.setValueAtTime(1, this.audioContext.currentTime + audioBuffer.duration - fadeDuration);
          gain.gain.linearRampToValueAtTime(0, this.audioContext.currentTime + audioBuffer.duration);
        }

        source.connect(gain);
        gain.connect(this.audioContext.destination);

        this.currentSource = source;
        source.onended = () => {
          this.currentSource = null;
          this._playNextAudio();
        };
        source.start();
      } catch (e) {
        console.warn('Oracle: Audio decode error', e);
        this._playNextAudio();
      }
    }

    stopAudio() {
      this.audioQueue = [];
      if (this.currentSource) {
        try { this.currentSource.stop(); } catch (e) {}
        this.currentSource = null;
      }
      this.isPlayingAudio = false;
      if (window.speechSynthesis) speechSynthesis.cancel();
    }

    /* ─── BROWSER TTS FALLBACK ─── */
    _speakFallback(text) {
      if (!window.speechSynthesis) return;
      const utterance = new SpeechSynthesisUtterance(text);
      utterance.rate = 0.95;
      utterance.pitch = 1.0;
      utterance.lang = 'en-US';
      this.setState('speaking');
      utterance.onend = () => { if (this.state === 'speaking') this.setState('idle'); };
      utterance.onerror = () => { if (this.state === 'speaking') this.setState('idle'); };
      speechSynthesis.speak(utterance);
    }

    /* ─── SPEECH RECOGNITION (STT) ─── */
    _initSpeechRecognition() {
      const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
      if (!SR) {
        this.els.micBtn.classList.add('unsupported');
        this.els.micBtn.title = 'Voice input not supported in this browser';
        return;
      }
      this.recognition = new SR();
      this.recognition.continuous = false;
      this.recognition.interimResults = true;
      this.recognition.lang = 'en-US';

      this.recognition.onresult = (e) => {
        let transcript = '';
        for (let i = e.resultIndex; i < e.results.length; i++) {
          transcript += e.results[i][0].transcript;
        }
        this.els.textInput.value = transcript;
        this.els.sendBtn.classList.toggle('active', transcript.trim().length > 0);

        // If final result, send
        if (e.results[e.results.length - 1].isFinal && transcript.trim()) {
          this.stopListening();
          this.els.textInput.value = '';
          this.els.sendBtn.classList.remove('active');
          this.sendMessage(transcript.trim());
        }
      };

      this.recognition.onerror = (e) => {
        if (e.error !== 'aborted' && e.error !== 'no-speech') {
          console.warn('Oracle STT error:', e.error);
        }
        this._stopRecordingUI();
      };

      this.recognition.onend = () => {
        this._stopRecordingUI();
      };
    }

    startListening() {
      if (!this.recognition || this.state === 'thinking' || this.state === 'speaking') return;
      this.stopAudio();

      try {
        this.recognition.start();
        this.isRecording = true;
        this.els.micBtn.classList.add('recording');
        this.els.waveform.classList.add('active');
        this.setState('listening');

        // Waveform visualization
        this._startWaveformViz();

        // Haptic
        if (navigator.vibrate) navigator.vibrate(30);
      } catch (e) {
        console.warn('Oracle: STT start error', e);
      }
    }

    stopListening() {
      if (!this.recognition) return;
      try { this.recognition.stop(); } catch (e) {}
      this._stopRecordingUI();
    }

    _stopRecordingUI() {
      this.isRecording = false;
      this.els.micBtn.classList.remove('recording');
      this.els.waveform.classList.remove('active');
      if (this._waveformRAF) cancelAnimationFrame(this._waveformRAF);
      if (this.state === 'listening') this.setState('idle');
    }

    /* ─── WAVEFORM VISUALIZATION ─── */
    _startWaveformViz() {
      const canvas = this.els.waveCanvas;
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const W = canvas.parentElement.clientWidth;
      canvas.width = W * 2;
      canvas.height = 64;
      canvas.style.width = W + 'px';

      const bars = 40;
      const barW = (W * 2) / bars - 2;

      const draw = () => {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        for (let i = 0; i < bars; i++) {
          const h = this.isRecording ? (8 + Math.random() * 48) : 4;
          const x = i * (barW + 2);
          const y = (64 - h) / 2;
          ctx.fillStyle = 'rgba(61, 140, 140, 0.6)';
          ctx.fillRect(x, y, barW, h);
        }
        if (this.isRecording) {
          this._waveformRAF = requestAnimationFrame(draw);
        }
      };
      draw();
    }

    /* ─── MESSAGES ─── */
    _appendMessage(role, text, streaming = false) {
      const msg = document.createElement('div');
      msg.className = 'oracle-msg oracle-msg-' + (role === 'ai' ? 'ai' : 'user');
      msg.innerHTML = `<div class="oracle-msg-indicator"></div><div class="oracle-msg-content">${streaming ? '' : this._escapeHtml(text)}</div>`;

      if (streaming) {
        const cursor = document.createElement('span');
        cursor.className = 'oracle-cursor';
        msg.querySelector('.oracle-msg-content').appendChild(cursor);
      }

      this.els.messages.appendChild(msg);
      this.els.messages.scrollTop = this.els.messages.scrollHeight;
      this.messages.push({ role, text });
      return msg;
    }

    _escapeHtml(t) {
      const d = document.createElement('div');
      d.textContent = t;
      return d.innerHTML;
    }

    /* ─── HEALTH CHECK ─── */
    async _checkHealth() {
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const res = await fetch(this.config.healthUrl, { signal: AbortSignal.timeout(3000) });
          if (res.ok) {
            this.backendAvailable = true;
            this.setState('idle');
            return;
          }
        } catch (e) {}
        if (attempt === 0) await new Promise(r => setTimeout(r, 1000));
      }
      this.backendAvailable = false;
      this.setState(this.config.enableMockMode ? 'idle' : 'offline');
      if (this.els.status) {
        this.els.status.textContent = this.config.enableMockMode ? 'Demo mode \u2014 offline' : 'Oracle unavailable';
      }
    }

    /* ─── FIRST VISIT PROMPT ─── */
    _showFirstVisit() {
      if (sessionStorage.getItem('oracle_visited')) return;
      sessionStorage.setItem('oracle_visited', '1');

      setTimeout(() => {
        if (this.panelOpen) return;
        const bubble = document.createElement('div');
        bubble.className = 'oracle-bubble';
        bubble.textContent = 'Hey \u2014 want to talk? Click me.';
        document.body.appendChild(bubble);
        requestAnimationFrame(() => { requestAnimationFrame(() => { bubble.classList.add('visible'); }); });
        setTimeout(() => {
          bubble.classList.remove('visible');
          setTimeout(() => bubble.remove(), 500);
        }, 5000);
      }, 3000);
    }
  }

  /* ─── AUTO-INITIALIZE ─── */
  document.addEventListener('DOMContentLoaded', function () {
    var cfg = window.METAPHY_ORACLE_CONFIG || {};
    window.oracle = new Oracle(cfg);
    window.oracle.render();

    // Auto-open Oracle panel if linked from Neurolux (?oracle=open)
    if (new URLSearchParams(window.location.search).get('oracle') === 'open') {
      setTimeout(function () { window.oracle.togglePanel(); }, 600);
    }
  });

})();
