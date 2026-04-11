/* ═══════════════════════════════════════════════════════════════════════════
   metaphy-nav.js  —  Shared navigation component for Metaphy LLC
   Canonical source: index.html (homepage)
   CSS:  assets/css/metaphy-core.css  §5 Navigation, §6 Hamburger, §7 Mobile
   ═══════════════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  /* ── Link definitions ─────────────────────────────────────────────────── */
  var NAV_LINKS = [
    { href: '/',                          label: 'Home' },
    { href: '/about-us/',                 label: 'About' },
    { href: '/mission-statement/',        label: 'Mission' },
    { href: '/core-beliefs/',             label: 'Beliefs' },
    { href: '/agent-codex/',              label: 'Agent Codex' },
    { href: '/services/',                 label: 'Services' },
    { href: '/projects/',                 label: 'Projects' }
  ];

  /* Mobile menu shows more links (Publications + NDA) */
  var MOBILE_EXTRA_LINKS = [
    { href: '/nda/',                      label: 'NDA' },
    { href: '/tachymetric-manifesto/',    label: 'Tachymetric Manifesto' },
    { href: '/architecture-of-harmony/',  label: 'Architecture of Harmony' },
    { href: '/synthesis-of-light/',       label: 'The Synthesis of Light' },
    { href: '/cryptography-of-being/',    label: 'Cryptography of Being' }
  ];

  /* ── Active-page detection ────────────────────────────────────────────── */
  function isActive(href) {
    var path = window.location.pathname;
    if (href === '/') return path === '/' || path === '/index.html';
    return path.indexOf(href) === 0;
  }

  /* ── Build link HTML ──────────────────────────────────────────────────── */
  function linkHTML(link, tag) {
    var cls = isActive(link.href) ? ' class="active"' : '';
    if (tag === 'li') return '<li><a href="' + link.href + '"' + cls + '>' + link.label + '</a></li>';
    return '<a href="' + link.href + '"' + cls + '>' + link.label + '</a>';
  }

  /* ── Nav HTML ─────────────────────────────────────────────────────────── */
  function buildNavHTML() {
    var desktopLinks = NAV_LINKS.map(function (l) { return linkHTML(l, 'li'); }).join('\n    ');
    var allLinks = NAV_LINKS.concat(MOBILE_EXTRA_LINKS);
    var mobileLinks = allLinks.map(function (l) { return linkHTML(l, 'a'); }).join('\n  ');

    return '' +
      '<a class="skip-to-content" href="#main">Skip to content</a>\n' +
      '<nav class="top-nav" id="topNav">\n' +
      '  <a href="/" class="nav-brand">Metaphysics &amp; Computing</a>\n' +
      '  <ul class="nav-links">\n' +
      '    ' + desktopLinks + '\n' +
      '  </ul>\n' +
      '  <button class="hamburger" id="hamburgerBtn" aria-label="Toggle menu" aria-expanded="false">\n' +
      '    <span></span><span></span><span></span>\n' +
      '  </button>\n' +
      '  <div class="nav-progress" id="navProgress"></div>\n' +
      '</nav>\n' +
      '<div class="mobile-menu" id="mobileMenu">\n' +
      '  ' + mobileLinks + '\n' +
      '</div>';
  }

  /* ── Inject ───────────────────────────────────────────────────────────── */
  function inject() {
    /* Find injection target — first <nav class="top-nav"> or body start */
    var existing = document.querySelector('nav.top-nav');
    var existingMobile = document.querySelector('.mobile-menu');

    /* If inline nav already exists, replace it */
    if (existing) {
      var frag = document.createElement('div');
      frag.innerHTML = buildNavHTML();
      var parent = existing.parentNode;
      /* Insert new elements before old nav */
      while (frag.firstChild) parent.insertBefore(frag.firstChild, existing);
      parent.removeChild(existing);
      if (existingMobile) existingMobile.parentNode.removeChild(existingMobile);
    } else {
      /* No existing nav — prepend to body */
      var wrapper = document.createElement('div');
      wrapper.innerHTML = buildNavHTML();
      var body = document.body;
      var ref = body.firstChild;
      while (wrapper.firstChild) body.insertBefore(wrapper.firstChild, ref);
    }

    bind();
  }

  /* ── Behaviour bindings ───────────────────────────────────────────────── */
  function bind() {
    var nav       = document.getElementById('topNav');
    var btn       = document.getElementById('hamburgerBtn');
    var menu      = document.getElementById('mobileMenu');
    var progress  = document.getElementById('navProgress');

    if (!nav) return;

    /* — Scroll: hide/show nav + progress bar — */
    var lastY = 0;
    window.addEventListener('scroll', function () {
      var y = window.scrollY;

      /* Hide on scroll-down (past 200px), show on scroll-up */
      if (y > lastY && y > 200) {
        nav.classList.add('hidden');
        closeMenu();
      } else {
        nav.classList.remove('hidden');
      }
      lastY = y;

      /* Progress bar */
      if (progress) {
        var max = document.documentElement.scrollHeight - window.innerHeight;
        progress.style.width = (max > 0 ? (y / max * 100) : 0) + '%';
      }
    }, { passive: true });

    /* — Hamburger toggle — */
    if (btn && menu) {
      btn.addEventListener('click', function () {
        var open = menu.classList.toggle('open');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      });

      /* Close on Escape */
      document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && menu.classList.contains('open')) {
          closeMenu();
        }
      });

      /* Close on outside click */
      document.addEventListener('click', function (e) {
        if (menu.classList.contains('open') &&
            !menu.contains(e.target) &&
            !btn.contains(e.target)) {
          closeMenu();
        }
      });
    }

    function closeMenu() {
      if (menu && btn) {
        menu.classList.remove('open');
        btn.setAttribute('aria-expanded', 'false');
      }
    }
  }

  /* ── Auto-init on DOM ready ───────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }

})();
