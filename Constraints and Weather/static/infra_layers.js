/* Toggleable infrastructure overlays for the constraint maps (shared by both tabs).
 * Sparse layers (wind/solar farms, data centers) load nationwide as static GeoJSON.
 * Dense layers (substations, pipelines) are zoom-gated and fetch only the current
 * viewport live from Overpass (via the Flask proxy), so the map never tries to draw
 * the whole country. All layers are OFF by default. Source: OpenStreetMap (ODbL).
 *
 *   initInfraLayers(map, chipsEl, hintEl)   // call once per map
 */
(function () {
  window.initInfraLayers = function (map, el, hintEl) {
    const LAYERS = [
      { k: "wind",        label: "Wind",      kind: "static", src: "/api/infra/generation",
        filter: f => f.properties.source === "wind",
        pstyle: { radius: 3, color: "#0e7490", weight: 1, fillColor: "#06b6d4", fillOpacity: 0.75 } },
      { k: "solar",       label: "Solar",     kind: "static", src: "/api/infra/generation",
        filter: f => f.properties.source === "solar",
        pstyle: { radius: 3, color: "#b45309", weight: 1, fillColor: "#f59e0b", fillOpacity: 0.75 } },
      { k: "datacenters", label: "Data centers", kind: "static", src: "/api/infra/datacenters",
        filter: () => true,
        pstyle: { radius: 4, color: "#9d174d", weight: 1, fillColor: "#db2777", fillOpacity: 0.85 } },
      { k: "substations", label: "Substations", kind: "dense", src: "/api/infra/dense/substations", minZoom: 9,
        pstyle: { radius: 3, color: "#334155", weight: 1, fillColor: "#94a3b8", fillOpacity: 0.85 } },
      { k: "pipelines",   label: "Pipelines",  kind: "dense", src: "/api/infra/dense/pipelines", minZoom: 9,
        lstyle: { color: "#ea580c", weight: 1.5, opacity: 0.75, dashArray: "4 3" } },
    ];
    const active = {}, groups = {}, staticCache = {};

    function swatch(l) { return l.pstyle ? l.pstyle.fillColor : l.lstyle.color; }
    function chips() {
      el.innerHTML = LAYERS.map(l =>
        `<button data-k="${l.k}" class="${active[l.k] ? "active" : ""}">` +
        `<span class="ic-dot" style="background:${swatch(l)}"></span>${l.label}</button>`).join("");
      el.querySelectorAll("button").forEach(b => b.onclick = () => toggle(b.dataset.k));
    }
    function group(k) { if (!groups[k]) groups[k] = L.layerGroup().addTo(map); return groups[k]; }
    function popup(f, l) {
      const p = f.properties || {};
      return `<b>${p.name || l.label}</b>` + (p.operator ? "<br>" + p.operator : "") +
        (p.mw ? "<br>" + p.mw + " MW" : "") + (p.source ? "<br>" + p.source : "") +
        (p.substance ? "<br>" + p.substance : "") + (p.voltage ? "<br>" + p.voltage + " kV" : "");
    }
    function draw(gj, l) {
      const g = group(l.k);
      L.geoJSON(gj, {
        filter: l.filter,
        pointToLayer: (f, ll) => L.circleMarker(ll, l.pstyle).bindPopup(popup(f, l)),
        style: l.lstyle,
        onEachFeature: (f, ly) => { if (f.geometry.type !== "Point") ly.bindPopup(popup(f, l)); },
      }).addTo(g);
    }
    async function loadStatic(l) {
      const g = group(l.k); g.clearLayers();
      let gj = staticCache[l.src];
      if (!gj) { gj = await (await fetch(l.src)).json(); staticCache[l.src] = gj; }
      draw(gj, l);
    }
    async function loadDense(l) {
      const g = group(l.k); g.clearLayers();
      if (map.getZoom() < l.minZoom) { showHint(); return; }
      const b = map.getBounds();
      const bbox = `${b.getSouth().toFixed(2)},${b.getWest().toFixed(2)},${b.getNorth().toFixed(2)},${b.getEast().toFixed(2)}`;
      try {
        const gj = await (await fetch(`${l.src}?bbox=${bbox}`)).json();
        if (gj && gj.features) draw(gj, l);
      } catch (e) { /* overpass hiccup -- ignore, will retry on next move */ }
    }
    function refresh(l) { if (active[l.k]) (l.kind === "static" ? loadStatic : loadDense)(l); }
    function toggle(k) {
      const l = LAYERS.find(x => x.k === k);
      active[k] = !active[k]; chips();
      if (active[k]) refresh(l);
      else if (groups[k]) { map.removeLayer(groups[k]); delete groups[k]; }
      showHint();
    }
    function showHint() {
      if (!hintEl) return;
      const need = LAYERS.some(l => l.kind === "dense" && active[l.k]) && map.getZoom() < 9;
      hintEl.textContent = need ? "zoom in to load substations / pipelines" : "";
    }
    // Re-fetch the dense (viewport) layers after pan/zoom, debounced.
    let t;
    map.on("moveend", () => {
      clearTimeout(t);
      t = setTimeout(() => { LAYERS.filter(l => l.kind === "dense" && active[l.k]).forEach(loadDense); showHint(); }, 350);
    });
    chips();
  };
})();
