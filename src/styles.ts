/* ═══════════════════════════════════════════
   NewFreedeck — CSS
   ═══════════════════════════════════════════ */

export const HOME_PANEL_CSS = `
.freedeck-home-root .freedeck-home-tab-pane{width:100%;min-height:100%;display:flex;flex:1 1 auto;flex-direction:column;padding-top:40px}
.freedeck-home-root .freedeck-home-preview-inner{width:100%;margin-left:0;min-height:100%;display:flex;flex:1 1 auto;flex-direction:column}
.freedeck-home-root .freedeck-home-preview-row{width:100%;display:flex;align-items:center;gap:12px}
.freedeck-home-root .freedeck-home-preview-cover{width:52px;min-width:52px;height:78px;border-radius:10px;overflow:hidden;background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);flex:0 0 auto}
.freedeck-home-root .freedeck-home-login-notice{width:100%;overflow:hidden;white-space:nowrap;color:#8b929a;font-size:11px;line-height:1.4;padding:4px 0;opacity:0.7}
.freedeck-home-root .freedeck-home-login-notice-track{display:inline-flex;animation:freedeck-login-notice-marquee 40s linear infinite}
.freedeck-home-root .freedeck-home-login-notice-text{display:inline-block;padding-right:48px}
@keyframes freedeck-login-notice-marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
`;

export const LIBRARY_CSS = `
.freedeck-library{width:100%;height:100vh;display:flex;flex-direction:column;background:#0e141b;color:#cac4d0}
.freedeck-library-header{display:flex;align-items:center;justify-content:space-between;padding:8px 16px;border-bottom:1px solid rgba(255,255,255,0.06);background:rgba(0,0,0,0.3);flex:0 0 auto}
.freedeck-library-header-title{font-size:18px;font-weight:700;color:#f4f8fc;margin:0;letter-spacing:0.02em}
.freedeck-library-empty{flex:1;display:flex;align-items:center;justify-content:center;padding:60px;color:#8b929a;font-size:13px}

/* ── GameCard (freedeck-plugin 移植) ── */
.freedeck-library-grid-card{position:relative;flex:0 0 auto;width:133px;min-width:133px}
.freedeck-library-grid-card-focus-state{z-index:9999!important}
.freedeck-library-grid-card-button{transition:background 100ms ease}
.freedeck-library-grid-card-button .Panel{border:none!important;background:transparent!important;box-shadow:none!important}
.freedeck-library-grid-card-frame{position:relative;width:100%;height:200px;overflow:visible;padding:0}
.freedeck-library-grid-card-backdrop{display:none}
.freedeck-library-grid-card-focus-state .freedeck-library-grid-card-backdrop{display:none}


.freedeck-library-grid-card-cover-wrap{position:relative;width:100%;height:200px;min-height:200px;max-height:200px;overflow:visible;background:rgba(255,255,255,0.04);display:flex;align-items:center;justify-content:center}
.freedeck-library-grid-card-focus-state .freedeck-library-grid-card-cover-wrap{}
.freedeck-library-grid-card-cover-wrap::after{content:"";position:absolute;inset:0;background:linear-gradient(180deg,rgba(255,255,255,.015),rgba(0,0,0,.08));opacity:.28;pointer-events:none;z-index:1}
.freedeck-library-grid-card-cover{display:block;width:100%;height:100%;object-fit:cover}
.freedeck-library-grid-card-sideinfo{position:absolute;top:50%;left:calc(100% + 6px);width:176px;display:grid;gap:6px;padding:10px 12px;opacity:0;pointer-events:none;transform:translate3d(-6px,-50%,0);transition:opacity 100ms ease,transform 120ms ease;z-index:2;box-sizing:border-box}
.freedeck-library-grid-card-focus-state .freedeck-library-grid-card-sideinfo{opacity:1;transform:translate3d(0,-50%,0)}
.freedeck-library-grid-card-sideinfo.is-sideinfo-left{left:auto;right:calc(100% + 6px);transform:translate3d(6px,-50%,0)}
.freedeck-library-grid-card-focus-state .freedeck-library-grid-card-sideinfo.is-sideinfo-left{transform:translate3d(0,-50%,0)}
.freedeck-library-grid-card-sideinfo-title{font-size:14px;line-height:1.3;font-weight:700;color:#f4f8fb;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-shadow:0 1px 2px rgba(0,0,0,.8),0 2px 8px rgba(0,0,0,.6)}
.freedeck-library-grid-card-sideinfo-subtitle{font-size:11px;line-height:1.35;font-weight:500;color:rgba(236,242,247,.92);max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-shadow:0 1px 2px rgba(0,0,0,.8)}
.freedeck-library-grid-card-sideinfo-size{font-size:11px;line-height:1.35;font-weight:600;color:rgba(188,223,244,.96);margin-top:2px;text-shadow:0 1px 2px rgba(0,0,0,.8)}

/* ── LoadMoreCard ── */
.freedeck-library-load-more-card{flex:0 0 auto}
.freedeck-library-load-more-card-frame{display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,0.03);border:1px dashed rgba(255,255,255,0.08);border-radius:4px}
.freedeck-library-load-more-card-inner{text-align:center}
.freedeck-library-load-more-card-title{font-size:14px;font-weight:600;color:#cac4d0}
.freedeck-library-load-more-card-subtitle{font-size:11px;color:#8b929a;margin-top:4px}
`;