/* ═══════════════════════════════════════════════════════════════════════════
   metaphy-footer.js  —  Shared footer component for Metaphy LLC
   Canonical source: index.html (homepage) lines 832-857
   CSS:  assets/css/metaphy-core.css  §8 Footer
   ═══════════════════════════════════════════════════════════════════════════ */

(function () {
  'use strict';

  /* ── Link definitions ─────────────────────────────────────────────────── */
  var COLUMNS = [
    {
      heading: 'Company',
      links: [
        { href: '/',                     label: 'Home' },
        { href: '/about-us/',            label: 'About Us' },
        { href: '/mission-statement/',   label: 'Mission Statement' },
        { href: '/core-beliefs/',        label: 'Core Beliefs' }
      ]
    },
    {
      heading: 'Services &amp; Research',
      links: [
        { href: '/agent-codex/',         label: 'Agent Codex' },
        { href: '/services/',            label: 'Services' },
        { href: '/projects/',            label: 'Projects' },
        { href: '/nda/',                 label: 'NDA' }
      ]
    },
    {
      heading: 'Publications',
      links: [
        { href: '/tachymetric-manifesto/',    label: 'Tachymetric Manifesto' },
        { href: '/architecture-of-harmony/',  label: 'Architecture of Harmony' },
        { href: '/synthesis-of-light/',       label: 'The Synthesis of Light' },
        { href: '/cryptography-of-being/',    label: 'Cryptography of Being' }
      ]
    }
  ];

  /* ── Build footer HTML ────────────────────────────────────────────────── */
  function buildFooterHTML() {
    var year = new Date().getFullYear();

    var cols = COLUMNS.map(function (col) {
      var links = col.links.map(function (l) {
        return '      <a href="' + l.href + '">' + l.label + '</a>';
      }).join('\n');
      return '    <div class="footer-col">\n' +
             '      <h4>' + col.heading + '</h4>\n' +
             links + '\n' +
             '    </div>';
    }).join('\n');

    return '' +
      '<footer class="site-footer">\n' +
      '  <div class="footer-nav">\n' +
      cols + '\n' +
      '  </div>\n' +
      '  <div class="footer-bottom">\n' +
      '    <p>Copyright &copy; ' + year + ' Metaphysics and Computing, a Metaphy LLC website.</p>\n' +
      '    <p><a href="https://www.x.com/MetaphyKing">@MetaphyKing</a> &middot; <a href="mailto:Logan@MetaphysicsandComputing.com">Logan@MetaphysicsandComputing.com</a></p>\n' +
      '  </div>\n' +
      '</footer>';
  }

  /* ── Inject ───────────────────────────────────────────────────────────── */
  function inject() {
    var existing = document.querySelector('footer.site-footer');

    if (existing) {
      /* Replace existing inline footer */
      var frag = document.createElement('div');
      frag.innerHTML = buildFooterHTML();
      var newFooter = frag.firstChild;
      existing.parentNode.replaceChild(newFooter, existing);
    } else {
      /* No existing footer — append before </body> */
      var wrapper = document.createElement('div');
      wrapper.innerHTML = buildFooterHTML();
      document.body.appendChild(wrapper.firstChild);
    }
  }

  /* ── Auto-init on DOM ready ───────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }
})();
