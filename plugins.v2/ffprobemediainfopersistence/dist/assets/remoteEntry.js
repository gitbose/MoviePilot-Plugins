const imports = {};
const allowed = new Set(['Module', '__esModule', 'default', '_export_sfc']);
const modules = {
  './Page': () => {
    loadCss(['__federation_expose_Page-CMbsz81m.css'], false, './Page');
    return load('./__federation_expose_Page-Cv4I2MIR.js').then(module => Object.keys(module).every(key => allowed.has(key)) ? () => module.default : () => module);
  },
  './Config': () => {
    loadCss(['__federation_expose_Config-EvTlcxFW.css'], false, './Config');
    return load('./__federation_expose_Config-laBQaIGV.js').then(module => Object.keys(module).every(key => allowed.has(key)) ? () => module.default : () => module);
  },
};
const loadedCss = {};
function loadCss(paths, defer, expose) {
  const root = import.meta.url.substring(0, import.meta.url.lastIndexOf('remoteEntry.js'));
  for (const path of paths) {
    const href = root + path;
    if (defer) { const key = 'css__FFprobeMediaInfoPersistence__' + expose; window[key] = window[key] || []; window[key].push(href); }
    else if (!loadedCss[href]) { loadedCss[href] = true; const link = document.createElement('link'); link.rel = 'stylesheet'; link.href = href; document.head.appendChild(link); }
  }
}
function load(path) { return imports[path] ??= import(path); }
function get(module) { if (!modules[module]) throw new Error('Can not find remote module ' + module); return modules[module](); }
function init(scope) {
  globalThis.__federation_shared__ = globalThis.__federation_shared__ || {};
  for (const [name, versions] of Object.entries(scope)) for (const [version, value] of Object.entries(versions)) {
    const target = value.scope || 'default'; const bucket = globalThis.__federation_shared__[target] = globalThis.__federation_shared__[target] || {}; (bucket[name] = bucket[name] || {})[version] = value;
  }
}
export { loadCss as dynamicLoadingCss, get, init };
